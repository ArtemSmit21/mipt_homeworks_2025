from fastapi import APIRouter, Query, Depends
from typing import Optional
from final_project.services.repository_service import RepositoryService
from final_project.infrastructure.github_client import GitHubClient

router = APIRouter(prefix="/api/search", tags=["search"])

def get_repository_service() -> RepositoryService:
    github_client = GitHubClient()
    return RepositoryService(github_client)


@router.get(
    "/repositories",
    summary="Поиск репозиториев на GitHub"
)
async def search_repositories(
        limit: int = Query(..., ge=1),
        offset: int = Query(default=0, ge=0),
        lang: str = Query(...),
        stars_min: int = Query(default=0, ge=0),
        stars_max: Optional[int] = Query(default=None, ge=0),
        forks_min: int = Query(default=0, ge=0),
        forks_max: Optional[int] = Query(default=None, ge=0),
        service: RepositoryService = Depends(get_repository_service)
):
    file_path = await service.search_and_save(
        limit=limit,
        offset=offset,
        lang=lang,
        stars_min=stars_min,
        stars_max=stars_max,
        forks_min=forks_min,
        forks_max=forks_max
    )
    return {
        "status": "success",
        "message": "Repositories saved successfully",
        "file_path": file_path,
        "count": limit
    }
