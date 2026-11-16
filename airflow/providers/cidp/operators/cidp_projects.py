"""Project operators (cf. ``airflow.providers.databricks.operators.databricks_repos``)."""

from __future__ import annotations

from collections.abc import Sequence

from airflow.providers.cidp.exceptions import CidpConflictError, CidpNotFoundError
from airflow.providers.cidp.operators.cidp import _CidpOperator, task_idempotency_key
from airflow.providers.cidp.version_compat import Context


class CidpProjectCreateOperator(_CidpOperator):
    """Create a project, optionally from a git repository (cf. ``DatabricksReposCreateOperator``).

    :param ignore_existing_project: answer the existing project instead of failing when the slug is taken.
    """

    template_fields: Sequence[str] = (
        "slug",
        "name",
        "description",
        "git_url",
        "branch",
        "template",
        "owner_group",
        "workspace",
        "cidp_conn_id",
    )

    def __init__(
        self,
        *,
        slug: str,
        name: str | None = None,
        description: str | None = None,
        git_url: str | None = None,
        branch: str | None = None,
        template: str | None = None,
        owner_group: str | None = None,
        ignore_existing_project: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.slug = slug
        self.name = name
        self.description = description
        self.git_url = git_url
        self.branch = branch
        self.template = template
        self.owner_group = owner_group
        self.ignore_existing_project = ignore_existing_project

    def execute(self, context: Context) -> dict:
        try:
            return self.hook.create_project(
                self.slug,
                name=self.name,
                description=self.description,
                git_url=self.git_url,
                branch=self.branch,
                template=self.template,
                owner_group=self.owner_group,
                idempotency_token=task_idempotency_key(context, "create-project", self.workspace, self.slug),
            )
        except CidpConflictError:
            if not self.ignore_existing_project:
                raise
            self.log.info("project %s exists; answering it", self.slug)
            return self.hook.get_project(self.slug)


class CidpProjectUpdateOperator(_CidpOperator):
    """Pull a project's branch from its git remote (cf. ``DatabricksReposUpdateOperator``)."""

    template_fields: Sequence[str] = ("project", "branch", "workspace", "cidp_conn_id")

    def __init__(self, *, project: str, branch: str = "main", **kwargs):
        super().__init__(**kwargs)
        self.project = project
        self.branch = branch

    def execute(self, context: Context) -> dict:
        return self.hook.pull_project(self.project, branch=self.branch)


class CidpProjectDeleteOperator(_CidpOperator):
    """Delete a project (cf. ``DatabricksReposDeleteOperator``).

    :param ignore_missing: succeed when the project does not exist.
    """

    template_fields: Sequence[str] = ("project", "workspace", "cidp_conn_id")

    def __init__(self, *, project: str, ignore_missing: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.project = project
        self.ignore_missing = ignore_missing

    def execute(self, context: Context) -> None:
        try:
            self.hook.delete_project(self.project)
        except CidpNotFoundError:
            if not self.ignore_missing:
                raise
            self.log.info("project %s does not exist", self.project)
