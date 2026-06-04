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
            for line in resp.iter_lines():
                if not line:
                    continue
                import json
                try:
                    data = json.loads(line)
                    if data.get("done"):
                        break
                except Exception:
                    continue
            resp.success()
