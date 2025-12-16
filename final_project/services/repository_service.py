import os
from typing import Optional
from aiofile import async_open

from final_project.infrastructure.github_client import GitHubClient


class RepositoryService:
    def __init__(self, github_client: GitHubClient):
        self.github_client = github_client

    def _build_query(
            self,
            lang: str,
            stars_min: int,
            stars_max: Optional[int],
            forks_min: int,
            forks_max: Optional[int]
    ) -> str:
        query_parts = [f"language:{lang}"]

        if stars_max is not None:
            query_parts.append(f"stars:{stars_min}..{stars_max}")
        else:
            query_parts.append(f"stars:>={stars_min}")
        if forks_max is not None:
            query_parts.append(f"forks:{forks_min}..{forks_max}")
        else:
            query_parts.append(f"forks:>={forks_min}")
        return " ".join(query_parts)

    async def search_and_save(
            self,
            limit: int,
            offset: int,
            lang: str,
            stars_min: int = 0,
            stars_max: Optional[int] = None,
            forks_min: int = 0,
            forks_max: Optional[int] = None
    ) -> str:

        query = self._build_query(lang, stars_min, stars_max, forks_min, forks_max)

        total_needed = limit + offset
        all_repos = []
        page = 1
        per_page = 100
        while len(all_repos) < total_needed:
            response = await self.github_client.search_repositories(
                query=query,
                page=page,
                per_page=per_page
            )
            items = response.get('items', [])
            if not items:
                break
            all_repos.extend(items)
            page += 1
            if len(items) < per_page:
                break

        selected_repos = all_repos[offset:offset + limit]
        csv_lines = []
        csv_lines.append("name,owner,stars,forks,url,description,language,created_at,updated_at")

        for repo in selected_repos:
            name = repo.get('name', '').replace(',', ';')
            owner = repo.get('owner', {}).get('login', '')
            stars = repo.get('stargazers_count', 0)
            forks = repo.get('forks_count', 0)
            url = repo.get('html_url', '')
            description = (repo.get('description') or '').replace(',', ';').replace('\n', ' ')
            language = repo.get('language', '')
            created_at = repo.get('created_at', '')
            updated_at = repo.get('updated_at', '')

            csv_lines.append(
                f"{name},{owner},{stars},{forks},{url},{description},{language},{created_at},{updated_at}"
            )
        filename = f"repositories_{lang}_{limit}_{offset}.csv"
        os.makedirs("static", exist_ok=True)
        filepath = os.path.join("static", filename)
        async with async_open(filepath, 'w', encoding='utf-8') as f:
            await f.write('\n'.join(csv_lines))
        return filepath
