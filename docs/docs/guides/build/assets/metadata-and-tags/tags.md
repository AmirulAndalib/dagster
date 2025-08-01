---
description: Organize and search Dagster assets using key-value pair tags.
sidebar_position: 1000
title: Tags
---

**Tags** are the primary way to organize assets in Dagster. You can attach several tags to an asset when it's defined, and they will appear in the UI. You can also use tags to search and filter for assets in the [Asset catalog](/guides/build/assets/asset-catalog) in Dagster+. They're structured as key-value pairs of strings.

Here's an example of some tags you might apply to an asset:

```python
{"domain": "marketing", "pii": "true"}
```

Like `owners`, just pass a dictionary of tags to the `tags` argument when defining an asset:

<CodeExample path="docs_snippets/docs_snippets/guides/data-modeling/metadata/tags.py" language="python" title="src/<project_name>/defs/assets.py" />

Keep in mind that tags must contain only strings as keys and values. Additionally, the Dagster UI will render tags with the empty string as a "label" rather than a key-value pair.

### Tag keys

Valid tag keys have two segments: an optional prefix and name, separated by a slash (`/`). Prefixes are typically expected to be Python package names. For example: `dagster/priority`

Prefixes and name segments must each:

- Be 63 characters or less
- Contain only alphanumeric characters, dashes (`-`), underscores (`_`), and dots (`.`)

### Tag values

Tag values must:

- Be 63 characters or less
- Contain only alphanumeric characters, dashes (`-`), underscores (`_`), and dots (`.`)
- Be a string or JSON that is serializable to a string

### Labels

A label is a tag that only contains a key. To create a label, set the tag value to an empty string:

```python
@dg.asset(
    tags={"private":""}
)
def my_asset() -> None:
    ...
```

A label will look like the following in the UI:

![Label in UI](/images/guides/build/assets/metadata-tags/label-ui.png)

### Customizing run execution with tags

While tags are primarily used for labeling and organization, some run execution features are controlled using run tags:

- [Customizing Kubernetes config](/deployment/oss/deployment-options/kubernetes/customizing-your-deployment)
- [Specifying Celery config](/deployment/oss/deployment-options/kubernetes/kubernetes-and-celery)
- [Setting concurrency limits when using the `QueuedRunCoordinator`](/guides/operate/managing-concurrency)
- [Setting the priority of different runs](/deployment/execution/customizing-run-queue-priority)

### System tags

#### Asset tags

The following table lists tags which Dagster may automatically add to assets.

| Tag                   | Description                                                                                                                   |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `dagster/kind/{kind}` | A tag identifying that an asset has a specific kind. For more information, see [Kind tags](/guides/build/assets/metadata-and-tags/kind-tags) |

### Run tags

The following table lists the tags Dagster will, on occasion, automatically add to runs.

| Tag                     | Description                         |
| ----------------------- | ----------------------------------- |
| `dagster/op_selection`  | The op selection for the run        |
| `dagster/partition`     | The partition of the run            |
| `dagster/schedule_name` | The schedule that triggered the run |
| `dagster/sensor_name`   | The sensor that triggered the run   |
| `dagster/backfill`      | The backfill ID                     |
| `dagster/parent_run_id` | The parent run of a re-executed run |
| `dagster/image`         | The Docker image tag                |
