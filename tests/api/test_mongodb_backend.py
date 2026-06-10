"""Mongo-only assertions at the collection level: documents really land.

Note the split this test pins down: metadata documents live in db "skeino"
(``MongoMetadataStore``'s default) while langgraph checkpoints live in db
"checkpointing_db" (``MongoDBSaver``'s default) — the db named in the URI path
is ignored by both.
"""

from pymongo import MongoClient

from tests.api.conftest import (
    MONGO_CHECKPOINT_DB,
    MONGO_METADATA_DB,
    Backend,
    api_client,
    create_thread,
    run_to_completion,
)


def test_documents_actually_in_mongo_and_delete_clears(
    mongodb_backend: Backend,
) -> None:
    client_db = MongoClient(mongodb_backend.uri)
    metadata_db = client_db.get_database(MONGO_METADATA_DB)
    checkpoint_db = client_db.get_database(MONGO_CHECKPOINT_DB)
    with api_client(mongodb_backend) as client:
        thread_id = create_thread(client, metadata={"check": "mongo"})
        run_to_completion(client, thread_id, "persist me")

        thread_doc = metadata_db["app_threads"].find_one({"_id": thread_id})
        assert thread_doc is not None
        assert thread_doc["status"] == "idle"
        run_doc = metadata_db["app_runs"].find_one({"thread_id": thread_id})
        assert run_doc is not None
        assert run_doc["status"] == "success"
        assert (
            checkpoint_db["checkpoints"].count_documents({"thread_id": thread_id}) >= 1
        )

        assert client.delete(f"/threads/{thread_id}").status_code == 204
        assert metadata_db["app_threads"].find_one({"_id": thread_id}) is None
        assert metadata_db["app_runs"].find_one({"thread_id": thread_id}) is None
        assert (
            checkpoint_db["checkpoints"].count_documents({"thread_id": thread_id}) == 0
        )
