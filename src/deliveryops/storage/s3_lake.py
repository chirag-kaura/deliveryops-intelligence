"""S3/MinIO URI mapper and hybrid object storage adapter with local filesystem fallback."""

from pathlib import Path

from deliveryops.config import AppSettings, get_settings
from deliveryops.storage.local_lake import LocalLakeStorage


class S3LakeAdapter:
    """Maps Medallion datasets to S3/MinIO bucket URIs and mirrors writes to LocalLakeStorage."""

    def __init__(
        self,
        local_lake: LocalLakeStorage,
        settings: AppSettings | None = None,
    ) -> None:
        self.local_lake = local_lake
        self.settings = settings or get_settings()

    def bucket_for_layer(self, layer: str) -> str:
        """Return the configured S3/MinIO bucket name for a Medallion layer."""
        mapping = {
            "bronze": self.settings.minio_bucket_bronze,
            "silver": self.settings.minio_bucket_silver,
            "gold": self.settings.minio_bucket_gold,
        }
        if layer not in mapping:
            raise ValueError(f"Unsupported Medallion layer: {layer}")
        return mapping[layer]

    def build_s3_uri(
        self,
        layer: str,
        dataset: str,
        partition_date: str,
        filename: str,
    ) -> str:
        """Construct a canonical Hive-partitioned s3:// URI for a dataset object."""
        bucket = self.bucket_for_layer(layer)
        return f"s3://{bucket}/{dataset}/dt={partition_date}/{filename}"

    def local_path_to_s3_uri(self, local_path: Path, layer: str, dataset: str) -> str:
        """Convert a local partitioned file path into its corresponding s3:// URI."""
        part_folder = local_path.parent.name
        dt_val = part_folder.replace("dt=", "")
        return self.build_s3_uri(
            layer=layer,
            dataset=dataset,
            partition_date=dt_val,
            filename=local_path.name,
        )
