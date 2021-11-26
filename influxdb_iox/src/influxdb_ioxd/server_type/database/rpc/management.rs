use data_types::{chunk_metadata::ChunkId, DatabaseName};
use generated_types::{
    google::{AlreadyExists, FieldViolation, FieldViolationExt, NotFound},
    influxdata::iox::management::v1::{Error as ProtobufError, *},
};
use query::QueryDatabase;
use server::{rules::ProvidedDatabaseRules, ApplicationState, Error, Server};
use std::{convert::TryFrom, sync::Arc};
use tonic::{Request, Response, Status};
use uuid::Uuid;

struct ManagementService {
    application: Arc<ApplicationState>,
    server: Arc<Server>,
}

use super::error::{
    default_database_error_handler, default_db_error_handler, default_server_error_handler,
};

#[tonic::async_trait]
impl management_service_server::ManagementService for ManagementService {
    async fn list_databases(
        &self,
        request: Request<ListDatabasesRequest>,
    ) -> Result<Response<ListDatabasesResponse>, Status> {
        let ListDatabasesRequest { omit_defaults } = request.into_inner();

        let rules = self
            .server
            .databases()
            .map_err(default_server_error_handler)?
            .into_iter()
            .filter_map(|db| db.provided_rules())
            .map(|rules| format_rules(rules, omit_defaults))
            .collect::<Vec<_>>();

        Ok(Response::new(ListDatabasesResponse { rules }))
    }

    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> Result<Response<GetDatabaseResponse>, Status> {
        let GetDatabaseRequest {
            name,
            omit_defaults,
        } = request.into_inner();

        let name = DatabaseName::new(name).scope("name")?;
        let database = self
            .server
            .database(&name)
            .map_err(default_server_error_handler)?;

        if !database.is_active() {
            return Err(NotFound {
                resource_type: "database".to_string(),
                resource_name: name.to_string(),
                ..Default::default()
            }
            .into());
        }

        let rules = database
            .provided_rules()
            .map(|rules| format_rules(rules, omit_defaults))
            .ok_or_else(|| {
                tonic::Status::unavailable(format!(
                    "Rules have not yet been loaded for database ({})",
                    name
                ))
            })?;

        Ok(Response::new(GetDatabaseResponse { rules: Some(rules) }))
    }

    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let rules: DatabaseRules = request
            .into_inner()
            .rules
            .ok_or_else(|| FieldViolation::required("rules"))?;

        let provided_rules =
            ProvidedDatabaseRules::new_rules(rules).map_err(|e| FieldViolation {
                field: "rules".to_string(),
                description: e.to_string(),
            })?;

        let database = self
            .server
            .create_database(provided_rules)
            .await
            .map_err(default_server_error_handler)?;

        let uuid = database
            .uuid()
            .expect("Database should be initialized or an error should have been returned");

        Ok(Response::new(CreateDatabaseResponse {
            uuid: uuid.as_bytes().to_vec(),
        }))
    }

    async fn update_database(
        &self,
        request: Request<UpdateDatabaseRequest>,
    ) -> Result<Response<UpdateDatabaseResponse>, Status> {
        let rules: DatabaseRules = request
            .into_inner()
            .rules
            .ok_or_else(|| FieldViolation::required("rules"))?;

        let provided_rules =
            ProvidedDatabaseRules::new_rules(rules).map_err(|e| FieldViolation {
                field: "rules".to_string(),
                description: e.to_string(),
            })?;

        let updated_rules = self
            .server
            .update_db_rules(provided_rules)
            .await
            .map_err(default_server_error_handler)?;

        Ok(Response::new(UpdateDatabaseResponse {
            rules: Some(updated_rules.rules().as_ref().clone().into()),
        }))
    }

    async fn release_database(
        &self,
        request: Request<ReleaseDatabaseRequest>,
    ) -> Result<Response<ReleaseDatabaseResponse>, Status> {
        let ReleaseDatabaseRequest { db_name, uuid } = request.into_inner();

        let db_name = DatabaseName::new(db_name).scope("db_name")?;
        let uuid = if uuid.is_empty() {
            None
        } else {
            Some(Uuid::from_slice(&uuid).scope("uuid")?)
        };

        let returned_uuid = self
            .server
            .release_database(&db_name, uuid)
            .await
            .map_err(default_server_error_handler)?;

        Ok(Response::new(ReleaseDatabaseResponse {
            uuid: returned_uuid.as_bytes().to_vec(),
        }))
    }

    async fn claim_database(
        &self,
        request: Request<ClaimDatabaseRequest>,
    ) -> Result<Response<ClaimDatabaseResponse>, Status> {
        let ClaimDatabaseRequest { uuid } = request.into_inner();

        let uuid = Uuid::from_slice(&uuid).scope("uuid")?;

        let db_name = self
            .server
            .claim_database(uuid)
            .await
            .map_err(default_server_error_handler)?;

        Ok(Response::new(ClaimDatabaseResponse {
            db_name: db_name.to_string(),
        }))
    }

    async fn list_detailed_databases(
        &self,
        _: Request<ListDetailedDatabasesRequest>,
    ) -> Result<Response<ListDetailedDatabasesResponse>, Status> {
        let databases = self
            .server
            .list_detailed_databases()
            .await
            .map_err(default_server_error_handler)?
            .into_iter()
            .map(Into::into)
            .collect();

        Ok(Response::new(ListDetailedDatabasesResponse { databases }))
    }

    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        let db_name = DatabaseName::new(request.into_inner().db_name).scope("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        let chunk_summaries = match db.chunk_summaries() {
            Ok(chunk_summaries) => chunk_summaries,
            Err(e) => return Err(default_db_error_handler(e)),
        };

        let chunks: Vec<Chunk> = chunk_summaries
            .into_iter()
            .map(|summary| summary.into())
            .collect();

        Ok(Response::new(ListChunksResponse { chunks }))
    }

    async fn create_dummy_job(
        &self,
        request: Request<CreateDummyJobRequest>,
    ) -> Result<Response<CreateDummyJobResponse>, Status> {
        let request = request.into_inner();
        let tracker = self
            .application
            .job_registry()
            .spawn_dummy_job(request.nanos, None);
        let operation = Some(super::operations::encode_tracker(tracker)?);
        Ok(Response::new(CreateDummyJobResponse { operation }))
    }

    async fn list_partitions(
        &self,
        request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        let ListPartitionsRequest { db_name } = request.into_inner();
        let db_name = DatabaseName::new(db_name).scope("db_name")?;

        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        let partition_keys = db.partition_keys().map_err(default_db_error_handler)?;
        let partitions = partition_keys
            .into_iter()
            .map(|key| Partition { key })
            .collect::<Vec<_>>();

        Ok(Response::new(ListPartitionsResponse { partitions }))
    }

    async fn get_partition(
        &self,
        request: Request<GetPartitionRequest>,
    ) -> Result<Response<GetPartitionResponse>, Status> {
        let GetPartitionRequest {
            db_name,
            partition_key,
        } = request.into_inner();
        let db_name = DatabaseName::new(db_name).scope("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        // TODO: get more actual partition details
        let partition_keys = db.partition_keys().map_err(default_db_error_handler)?;

        let partition = if partition_keys.contains(&partition_key) {
            Some(Partition { key: partition_key })
        } else {
            None
        };

        Ok(Response::new(GetPartitionResponse { partition }))
    }

    async fn list_partition_chunks(
        &self,
        request: Request<ListPartitionChunksRequest>,
    ) -> Result<Response<ListPartitionChunksResponse>, Status> {
        let ListPartitionChunksRequest {
            db_name,
            partition_key,
        } = request.into_inner();
        let db_name = DatabaseName::new(db_name).scope("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        let chunks: Vec<Chunk> = db
            .partition_chunk_summaries(&partition_key)
            .into_iter()
            .map(|summary| summary.into())
            .collect();

        Ok(Response::new(ListPartitionChunksResponse { chunks }))
    }

    async fn new_partition_chunk(
        &self,
        request: Request<NewPartitionChunkRequest>,
    ) -> Result<Response<NewPartitionChunkResponse>, Status> {
        let NewPartitionChunkRequest {
            db_name,
            partition_key,
            table_name,
        } = request.into_inner();
        let db_name = DatabaseName::new(db_name).scope("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        db.rollover_partition(&table_name, &partition_key)
            .await
            .map_err(default_db_error_handler)?;

        Ok(Response::new(NewPartitionChunkResponse {}))
    }

    async fn close_partition_chunk(
        &self,
        request: Request<ClosePartitionChunkRequest>,
    ) -> Result<Response<ClosePartitionChunkResponse>, Status> {
        let ClosePartitionChunkRequest {
            db_name,
            partition_key,
            table_name,
            chunk_id,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).scope("db_name")?;

        let chunk_id = ChunkId::try_from(chunk_id).scope("chunk_id")?;

        let tracker = self
            .server
            .close_chunk(&db_name, table_name, partition_key, chunk_id)
            .map_err(default_server_error_handler)?;

        let operation = Some(super::operations::encode_tracker(tracker)?);

        Ok(Response::new(ClosePartitionChunkResponse { operation }))
    }

    async fn unload_partition_chunk(
        &self,
        request: tonic::Request<UnloadPartitionChunkRequest>,
    ) -> Result<tonic::Response<UnloadPartitionChunkResponse>, tonic::Status> {
        let UnloadPartitionChunkRequest {
            db_name,
            partition_key,
            table_name,
            chunk_id,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).scope("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        let chunk_id = ChunkId::try_from(chunk_id).scope("chunk_id")?;

        db.unload_read_buffer(&table_name, &partition_key, chunk_id)
            .map_err(default_db_error_handler)?;

        Ok(Response::new(UnloadPartitionChunkResponse {}))
    }

    async fn get_server_status(
        &self,
        _request: Request<GetServerStatusRequest>,
    ) -> Result<Response<GetServerStatusResponse>, Status> {
        let initialized = self.server.initialized();

        // Purposefully suppress error from server::Databases as don't want
        // to return an error if the server is not initialized
        let mut database_statuses: Vec<_> = self
            .server
            .databases()
            .map(|databases| {
                databases
                    .into_iter()
                    .map(|database| DatabaseStatus {
                        db_name: database.config().name.to_string(),
                        error: database.init_error().map(|e| ProtobufError {
                            message: e.to_string(),
                        }),
                        state: database.state_code().into(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Sort output by database name
        database_statuses.sort_unstable_by(|a, b| a.db_name.cmp(&b.db_name));

        Ok(Response::new(GetServerStatusResponse {
            server_status: Some(ServerStatus {
                initialized,
                error: self.server.server_init_error().map(|e| ProtobufError {
                    message: e.to_string(),
                }),
                database_statuses,
            }),
        }))
    }

    async fn wipe_preserved_catalog(
        &self,
        request: Request<WipePreservedCatalogRequest>,
    ) -> Result<Response<WipePreservedCatalogResponse>, Status> {
        let WipePreservedCatalogRequest { db_name } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).scope("db_name")?;

        let tracker = self
            .server
            .wipe_preserved_catalog(&db_name)
            .map_err(|e| match e {
                Error::DatabaseAlreadyExists { db_name } => AlreadyExists {
                    resource_type: "database".to_string(),
                    resource_name: db_name,
                    ..Default::default()
                }
                .into(),
                e => default_server_error_handler(e),
            })?;
        let operation = Some(super::operations::encode_tracker(tracker)?);

        Ok(Response::new(WipePreservedCatalogResponse { operation }))
    }

    async fn skip_replay(
        &self,
        request: Request<SkipReplayRequest>,
    ) -> Result<Response<SkipReplayResponse>, Status> {
        let SkipReplayRequest { db_name } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).scope("db_name")?;

        let database = self
            .server
            .database(&db_name)
            .map_err(default_server_error_handler)?;

        database
            .skip_replay()
            .await
            .map_err(default_database_error_handler)?;

        Ok(Response::new(SkipReplayResponse {}))
    }

    async fn persist_partition(
        &self,
        request: tonic::Request<PersistPartitionRequest>,
    ) -> Result<tonic::Response<PersistPartitionResponse>, tonic::Status> {
        let PersistPartitionRequest {
            db_name,
            partition_key,
            table_name,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).scope("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        db.persist_partition(&table_name, &partition_key, false)
            .await
            .map_err(default_db_error_handler)?;

        Ok(Response::new(PersistPartitionResponse {}))
    }

    async fn drop_partition(
        &self,
        request: tonic::Request<DropPartitionRequest>,
    ) -> Result<tonic::Response<DropPartitionResponse>, tonic::Status> {
        let DropPartitionRequest {
            db_name,
            partition_key,
            table_name,
        } = request.into_inner();

        // Validate that the database name is legit
        let db_name = DatabaseName::new(db_name).scope("db_name")?;
        let db = self
            .server
            .db(&db_name)
            .map_err(default_server_error_handler)?;

        db.drop_partition(&table_name, &partition_key)
            .await
            .map_err(default_db_error_handler)?;

        Ok(Response::new(DropPartitionResponse {}))
    }
}

/// Returns [`DatabaseRules`] formated according to the `omit_defaults` flag. If `omit_defaults` is
/// true, returns the stored config, otherwise returns the actual configuration.
fn format_rules(provided_rules: Arc<ProvidedDatabaseRules>, omit_defaults: bool) -> DatabaseRules {
    if omit_defaults {
        // return rules as originally provided by the user
        provided_rules.original().clone()
    } else {
        // return the active rules (which have all default values filled in)
        provided_rules.rules().as_ref().clone().into()
    }
}

pub fn make_server(
    application: Arc<ApplicationState>,
    server: Arc<Server>,
) -> management_service_server::ManagementServiceServer<
    impl management_service_server::ManagementService,
> {
    management_service_server::ManagementServiceServer::new(ManagementService {
        application,
        server,
    })
}
