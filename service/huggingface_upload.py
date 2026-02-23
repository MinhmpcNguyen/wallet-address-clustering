import os
import time
from typing import Any

from huggingface_hub import (
    HfApi,
    upload_file,
    whoami,  # pyright: ignore [reportUnknownVariableType]
)

from utils.logger_utils import get_logger

logger = get_logger("HuggingFaceUploader")


class HuggingFaceUploader:
    """
    Convenience uploader for Hugging Face Hub (default `repo_type='model'`).

    - Supports pushing to a personal namespace or an organization.
    - Creates the repository if it doesn't exist (`exist_ok=True`), then uploads file(s).
    """

    def __init__(
        self,
        token: str | None,
        org_name: str | None,
        private_default: bool = False,
    ):
        """
        Args:
            token: HF token. If None, read from environment.
            org_name: Organization name (to push under org instead of the user namespace).
            private_default: Default repository visibility (can be overridden during upload).
        """
        token_env = os.getenv("HF_API_KEY")
        if not token_env:
            logger.error("Missing HF token. Please set HF_API_KEY.")
            raise RuntimeError("Missing HF token. Set HF_API_KEY.")

        logger.debug("Using token from env: HF_API_KEY")

        self.token: str = token_env
        self.api: HfApi = HfApi(token=self.token)
        try:
            self.identity: dict[Any, Any] = whoami(token=self.token)
            self.username: str = str(
                self.identity.get("name") or self.identity.get("email") or "anonymous"
            )
            logger.info("Authenticated to Hugging Face Hub as '%s'", self.username)
        except Exception:
            logger.error(
                "Failed to authenticate with the provided HF token", exc_info=True
            )
            raise

        self.org_name: str | None = org_name
        self.private_default: bool = private_default
        if self.org_name:
            logger.info(
                "Uploads will target organization namespace: %s (private_default=%s)",
                self.org_name,
                self.private_default,
            )
        else:
            logger.info(
                "Uploads will target user namespace: %s (private_default=%s)",
                self.username,
                self.private_default,
            )

    def _resolve_repo_id(self, repo_basename: str) -> str:
        """Decide repo_id based on org (if provided) or user namespace."""
        return (
            f"{self.org_name}/{repo_basename}"
            if self.org_name
            else f"{self.username}/{repo_basename}"
        )

    def ensure_repo(self, repo_basename: str) -> str:
        """
        Create repo if needed (`exist_ok=True`) and return the `repo_id`.
        """
        repo_id = self._resolve_repo_id(repo_basename)
        logger.info(
            "Ensuring repository exists: %s (repo_type=model, private=%s)",
            repo_id,
            self.private_default,
        )
        try:
            _ = self.api.create_repo(
                repo_id=repo_id,
                repo_type="model",
                private=self.private_default,
                exist_ok=True,
            )
        except Exception as e:
            logger.warning("[HF] create_repo warning for '%s': %s", repo_id, e)

        # Verify repo is accessible
        try:
            _ = self.api.repo_info(repo_id=repo_id, repo_type="model")
        except Exception:
            logger.error(
                "Repo '%s' is not accessible after creation attempt",
                repo_id,
                exc_info=True,
            )
            raise
        logger.debug("Repository is ready: %s", repo_id)
        return repo_id

    def upload_file(
        self,
        model_path: str,
        repo_basename: str,
        *,
        path_in_repo: str,
        repo_type: str = "model",
    ) -> str:
        """
        Upload a single file to a Hugging Face repo.

        Args:
            model_path: Local file path.
            repo_basename: Repo name (without namespace).
            path_in_repo: File path/name inside repo (defaults to basename of `model_path`).
            repo_type: HF repo type (default: 'model').

        Returns:
            Public URL of the HF repository.
        """
        if not os.path.isfile(model_path):
            logger.error("File not found: %s", model_path)
            raise FileNotFoundError(f"File not found: {model_path}")

        repo_id = self.ensure_repo(repo_basename)
        remote_path = path_in_repo or os.path.basename(model_path)

        file_size = os.path.getsize(model_path)
        t0 = time.time()
        logger.info(
            "Uploading file to HF: local='%s' (%d bytes) -> repo='%s' path_in_repo='%s'",
            model_path,
            file_size,
            repo_id,
            remote_path,
        )
        try:
            _ = upload_file(
                path_or_fileobj=model_path,
                path_in_repo=remote_path,
                repo_id=repo_id,
                repo_type=repo_type,
                token=self.token,
            )
        except Exception:
            logger.error(
                "Upload failed for '%s' -> %s/%s",
                model_path,
                repo_id,
                remote_path,
                exc_info=True,
            )
            raise

        elapsed = time.time() - t0
        url = f"https://huggingface.co/{repo_id}"
        logger.info(
            "[HF] Uploaded '%s' -> %s/%s (%.2fs)", model_path, url, remote_path, elapsed
        )
        return url

    def upload_files(self, files: list[tuple[str, str]], repo_basename: str) -> str:
        """
        Upload multiple files.

        Args:
            files: List of tuples `(local_path, path_in_repo or None)`.
            repo_basename: Repo name (without namespace).

        Returns:
            Public URL of the HF repository.
        """
        repo_id = self.ensure_repo(repo_basename)
        base_url = f"https://huggingface.co/{repo_id}"

        total = len(files)
        uploaded = 0
        t0 = time.time()
        for local_path, path_in_repo in files:
            if not os.path.isfile(local_path):
                logger.error("File not found: %s", local_path)
                raise FileNotFoundError(f"File not found: {local_path}")
            remote_path = path_in_repo or os.path.basename(local_path)
            try:
                _ = upload_file(
                    path_or_fileobj=local_path,
                    path_in_repo=remote_path,
                    repo_id=repo_id,
                    repo_type="model",
                    token=self.token,
                )
                uploaded += 1
                logger.info(
                    "[HF] Uploaded '%s' -> %s/%s (%d/%d)",
                    local_path,
                    base_url,
                    remote_path,
                    uploaded,
                    total,
                )
            except Exception:
                logger.error(
                    "Upload failed for '%s' -> %s/%s (%d/%d)",
                    local_path,
                    base_url,
                    remote_path,
                    uploaded + 1,
                    total,
                    exc_info=True,
                )
                raise

        logger.info(
            "Completed uploading %d/%d files in %.2fs",
            uploaded,
            total,
            time.time() - t0,
        )
        return base_url
