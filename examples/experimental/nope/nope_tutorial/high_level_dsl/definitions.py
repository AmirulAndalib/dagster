from pathlib import Path
from typing import Any, Dict, List, Union

from dagster import _check as check
from dagster._core.definitions.asset_spec import AssetSpec
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.definitions.factory.executable import (
    AssetGraphExecutable,
    AssetGraphExecutionContext,
    AssetGraphExecutionResult,
)
from dagster._nope.definitions import (
    DefinitionsBuilder,
    InMemoryManifestSource,
    make_nope_definitions,
)
from manifest import (
    BespokeELTAssetManifest,
    BespokeELTExecutableManifest,
    DbtExecutableManifest,
    HighLevelDSLExecutableList,
    HighLevelDSLManifest,
)
from manifest_source import HighLevelDSLFileSystemManifestSource


def definitions_from_executables(
    executables: List[AssetGraphExecutable], resources: Dict[str, Any]
) -> Definitions:
    return Definitions(
        assets=[executable.to_assets_def() for executable in executables], resources=resources
    )


class HighLevelDSLDefsBuilder(DefinitionsBuilder):
    @classmethod
    def build(cls, manifest: HighLevelDSLManifest, resources: Dict[str, Any]) -> Definitions:
        manifest_file = check.inst(manifest.executable_manifest_file, HighLevelDSLExecutableList)
        return definitions_from_executables(
            executables=[
                cls.create_executable(harness, manifest.group_name)
                for harness in manifest_file.executables
            ],
            resources=resources,
        )

    @classmethod
    def create_executable(
        cls, harness: Union[BespokeELTExecutableManifest, DbtExecutableManifest], group_name: str
    ) -> AssetGraphExecutable:
        if harness.kind == "bespoke_elt":
            return BespokeELTExecutable(group_name=group_name, manifest=harness)
        elif harness.kind == "dbt_manifest":
            return DbtManifestExecutable(group_name=group_name, manifest=harness)
        else:
            raise NotImplementedError(f"Unknown kind {harness.kind}")


class DbtManifestExecutable(AssetGraphExecutable):
    def __init__(self, group_name: str, manifest: DbtExecutableManifest):
        super().__init__(
            specs=[AssetSpec(key=asset_key, group_name=group_name) for asset_key in ["hardcoded"]]
        )

    def execute(self, context: AssetGraphExecutionContext) -> AssetGraphExecutionResult:
        raise NotImplementedError("Not implemented")


class BespokeELTExecutable(AssetGraphExecutable):
    def __init__(self, group_name: str, manifest: BespokeELTExecutableManifest):
        super().__init__(
            specs=[
                AssetSpec(key=asset_key, group_name=group_name)
                for asset_key in manifest.assets.keys()
            ]
        )

    def execute(self, context: AssetGraphExecutionContext) -> AssetGraphExecutionResult:
        raise NotImplementedError("Not implemented")


def make_definitions_from_python_api() -> Definitions:
    return make_nope_definitions(
        manifest_source=InMemoryManifestSource(
            HighLevelDSLManifest(
                group_name="group_a",
                executable_manifest_file=HighLevelDSLExecutableList(
                    executables=[
                        BespokeELTExecutableManifest(
                            name="transform_and_load",
                            kind="bespoke_elt",
                            source="file://example/file.csv",
                            destination="s3://bucket/file.csv",
                            assets={
                                "root_one": BespokeELTAssetManifest(deps=None),
                                "root_two": BespokeELTAssetManifest(deps=None),
                            },
                        )
                    ]
                ),
            )
        ),
        defs_builder_cls=HighLevelDSLDefsBuilder,
        resources={},
    )


defs = make_nope_definitions(
    manifest_source=HighLevelDSLFileSystemManifestSource(
        path=Path(__file__).resolve().parent / Path("defs")
    ),
    defs_builder_cls=HighLevelDSLDefsBuilder,
    resources={},
)


assert make_definitions_from_python_api()


if __name__ == "__main__":
    assert isinstance(defs, Definitions)
