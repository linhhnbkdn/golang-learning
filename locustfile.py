import json
import os
import time
import uuid

import jwt
from locust import HttpUser, between, events, task
from locust.clients import ResponseContextManager

JWT_SECRET = os.getenv("JWT_SECRET", "secret")


def make_token(user_id: str) -> str:
    return jwt.encode(
        {"user_id": user_id, "exp": int(time.time()) + 86400},
        JWT_SECRET,
        algorithm="HS256",
    )


class ChatUser(HttpUser):
    wait_time = between(1, 3)

    def on_start(self):
        self.uid = str(uuid.uuid4())[:8]
        self.session_id = f"bench-{self.uid}"
        self.token = make_token(self.uid)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    @task
    def e2e_chat(self):
        start = time.perf_counter()

        with self.client.post(
            f"/chat/{self.session_id}",
            json={"content": "xin chao"},
            headers=self.headers,
            stream=True,
            catch_response=True,
            name="/chat/:session_id",
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"POST /chat failed: {resp.status_code}")
                return

            got_done = False
            for line in resp.iter_lines():
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    if data.get("done"):
                        got_done = True
                        break
                except Exception:
                    continue

            elapsed_ms = (time.perf_counter() - start) * 1000

            if not got_done:
                resp.failure("stream ended without done=true")
                return

            # Fire manual event để Locust record đúng full streaming time
            events.request.fire(
                request_type="POST",
                name="/chat/:session_id [full-stream]",
                response_time=elapsed_ms,
                response_length=0,
                exception=None,
                context={},
            )
            resp.success()
