import time
import os
import jwt
from locust import HttpUser, task, between


JWT_SECRET = os.getenv("JWT_SECRET", "secret")
SESSION_ID = "benchmark-session"


def make_token(user_id: str) -> str:
    return jwt.encode(
        {"user_id": user_id, "exp": int(time.time()) + 86400},
        JWT_SECRET,
        algorithm="HS256",
    )


class ChatUser(HttpUser):
    wait_time = between(1, 3)

    def on_start(self):
        self.token = make_token(f"user-{self.user_id}")
        self.headers = {"Authorization": f"Bearer {self.token}"}

    @task
    def e2e_chat(self):
        # Bước 1: gửi message
        with self.client.post(
            "/chat",
            json={"session_id": SESSION_ID, "content": "xin chao"},
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
        start = time.time()
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
            elapsed = (time.time() - start) * 1000
            resp.success()
