import httpx
from typing import Dict, Any


class GitHubClient:
    def __init__(self):
        self.base_url = "https://api.github.com"
        self.headers = {
            "User-Agent": "FastAPI-Student-Project",
            "Accept": "application/vnd.github.v3+json"
        }
        self.client = httpx.AsyncClient(timeout=30.0, headers=self.headers)

    async def search_repositories(
            self,
            query: str,
            page: int = 1,
            per_page: int = 100
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/search/repositories"
        params = {
            "q": query,
            "page": page,
            "per_page": per_page,
            "sort": "stars",
            "order": "desc"
        }

        response = await self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    async def close(self):
        await self.client.aclose()
