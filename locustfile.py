import time
import os
import uuid
import jwt
from locust import HttpUser, task, between


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
        # Bước 1: gửi message
        with self.client.post(
            "/chat",
            json={"session_id": self.session_id, "content": "xin chao"},
            headers=self.headers,
            catch_response=True,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"POST /chat failed: {resp.status_code}")
                return
            request_id = resp.json().get("request_id")
            if not request_id:
                resp.failure("missing request_id")
                return
            resp.success()

        # Bước 2: stream response
        with self.client.get(
            f"/chat/stream/{request_id}",
            headers=self.headers,
            stream=True,
            catch_response=True,
            name="/chat/stream/:request_id",
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"GET /stream failed: {resp.status_code}")
                return
            for line in resp.iter_lines():
                if line == b"data: [DONE]":
                    break
            resp.success()
