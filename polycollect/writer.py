"""Buffered Parquet writer that starts a new sequence of files every UTC day."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class DailyParquetWriter:
    """
    Buffers rows and writes them to ``{base_dir}/{YYYY-MM-DD}/{name}``.

    Rows are flushed to a ``.tmp`` file every ``flush_threshold`` rows.
    ``seal()`` closes it (writing the Parquet footer) and renames it to
    ``.parquet``; the next row opens the next part: ``events.parquet``,
    ``events.part1.parquet``, ... Existing files are never overwritten, so a
    restart continues the day's sequence. A crash loses only the rows written
    since the last seal, since a ``.tmp`` file has no footer.
    """

    def __init__(self, base_dir: str, name: str, schema: pa.Schema, flush_threshold: int = 1000):
        self._base_dir = Path(base_dir)
        self._name = name
        self._schema = schema
        self._flush_threshold = flush_threshold
        self._buffer: List[Dict] = []
        self._date: Optional[str] = None  # UTC day of the buffered rows
        self._writer: Optional[pq.ParquetWriter] = None
        self._tmp_path: Optional[Path] = None
        self._final_path: Optional[Path] = None

    def _today_utc(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def append(self, row: Dict):
        """Buffer one row. Keys not in the schema are ignored; missing ones are null."""
        today = self._today_utc()
        if today != self._date:
            self.seal()  # UTC midnight: finish the previous day's file
            self._date = today
        self._buffer.append(row)
        if len(self._buffer) >= self._flush_threshold:
            self._flush()

    def seal(self) -> Optional[Path]:
        """Write out buffered rows, close the file and rename .tmp → .parquet. Returns the sealed path."""
        self._flush()
        if self._writer is None:
            return None
        writer, self._writer = self._writer, None
        try:
            writer.close()
            os.replace(self._tmp_path, self._final_path)
        except (ValueError, OSError) as e:
            logger.error(f"Sealing {self._tmp_path} failed: {e}")
            return None
        logger.debug(f"Sealed {self._final_path}")
        return self._final_path

    def _open(self):
        """Open the first unused part name for the current day."""
        path = self._base_dir / self._date / self._name
        path.parent.mkdir(parents=True, exist_ok=True)
        final, part = path, 0
        while final.exists():
            part += 1
            final = path.with_name(f"{path.stem}.part{part}.parquet")
        self._final_path = final
        self._tmp_path = final.with_suffix(".tmp")
        self._writer = pq.ParquetWriter(str(self._tmp_path), self._schema, compression="zstd")

    def _flush(self):
        if not self._buffer:
            return
        rows, self._buffer = self._buffer, []
        try:
            table = pa.Table.from_pylist(rows, schema=self._schema)
        except (pa.ArrowException, TypeError, ValueError) as e:
            logger.error(f"Dropped {len(rows)} rows that don't fit the schema: {e}")
            return
        try:
            if self._writer is None:
                self._open()
            self._writer.write_table(table)
        except (ValueError, OSError) as e:
            # Drop the broken writer so the next flush starts a fresh file
            logger.error(f"Writing {self._tmp_path} failed, dropped {table.num_rows} rows: {e}")
            self._writer = None
