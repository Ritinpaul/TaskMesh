def test_task_create_get_and_idempotent_reuse(client, fake_producer):
    request_payload = {
        "task_type": "default",
        "idempotency_key": "duplicate-key-1",
        "payload": {"order": 42},
    }

    first = client.post("/tasks", json=request_payload)
    assert first.status_code == 201
    first_data = first.json()
    assert first_data["reused"] is False
    assert first_data["status"] == "queued"
    assert len(fake_producer.messages) == 1

    second = client.post("/tasks", json=request_payload)
    assert second.status_code == 200
    second_data = second.json()
    assert second_data["task_id"] == first_data["task_id"]
    assert second_data["reused"] is True
    assert len(fake_producer.messages) == 1

    lookup = client.get(f"/tasks/{first_data['task_id']}")
    assert lookup.status_code == 200
    detail = lookup.json()
    assert detail["task_id"] == first_data["task_id"]
    assert detail["idempotency_key"] == request_payload["idempotency_key"]
    assert detail["payload"] == request_payload["payload"]
    assert detail["status"] == "queued"
    assert detail["attempts"] == []
