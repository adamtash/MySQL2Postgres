"""
Status Monitoring Module

Provides real-time status monitoring and progress tracking for migration operations.
"""

import time
import threading
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Optional, Any, List
from dataclasses import dataclass, field


class MigrationPhase(Enum):
    """Phases of the migration process."""
    INITIALIZING = "INITIALIZING"
    CONNECTING = "CONNECTING"
    ANALYZING = "ANALYZING"
    MIGRATING = "MIGRATING"
    VALIDATING = "VALIDATING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TableStatus(Enum):
    """Status of individual tables during migration."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class TableProgress:
    """Progress information for a single table."""
    name: str
    status: TableStatus = TableStatus.PENDING
    total_records: int = 0
    processed_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    batch_count: int = 0
    current_batch: int = 0
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.total_records == 0:
            return 0.0
        return min(100.0, (self.processed_records / self.total_records) * 100)
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Calculate duration if started."""
        if self.start_time is None:
            return None
        end = self.end_time or datetime.now()
        return end - self.start_time
    
    @property
    def records_per_second(self) -> float:
        """Calculate processing rate."""
        duration = self.duration
        if duration is None or duration.total_seconds() == 0:
            return 0.0
        return self.processed_records / duration.total_seconds()


@dataclass
class MigrationStats:
    """Overall migration statistics."""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    phase: MigrationPhase = MigrationPhase.INITIALIZING
    total_tables: int = 0
    completed_tables: int = 0
    failed_tables: int = 0
    skipped_tables: int = 0
    total_records: int = 0
    processed_records: int = 0
    errors_count: int = 0
    warnings_count: int = 0
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Calculate total duration."""
        if self.start_time is None:
            return None
        end = self.end_time or datetime.now()
        return end - self.start_time
    
    @property
    def overall_progress(self) -> float:
        """Calculate overall progress percentage."""
        if self.total_records == 0:
            return 0.0
        return min(100.0, (self.processed_records / self.total_records) * 100)
    
    @property
    def table_progress(self) -> float:
        """Calculate table completion percentage."""
        if self.total_tables == 0:
            return 0.0
        return min(100.0, (self.completed_tables / self.total_tables) * 100)
    
    @property
    def records_per_second(self) -> float:
        """Calculate overall processing rate."""
        duration = self.duration
        if duration is None or duration.total_seconds() == 0:
            return 0.0
        return self.processed_records / duration.total_seconds()


class StatusMonitor:
    """Monitors and tracks migration status and progress."""
    
    def __init__(self):
        self.stats = MigrationStats()
        self.table_progress: Dict[str, TableProgress] = {}
        self.lock = threading.Lock()
        self._phase_start_times: Dict[MigrationPhase, datetime] = {}
        self._last_update = datetime.now()
        
    def start_migration(self, table_names: List[str]) -> None:
        """
        Initialize migration monitoring.
        
        Args:
            table_names: List of tables to migrate
        """
        with self.lock:
            self.stats.start_time = datetime.now()
            self.stats.phase = MigrationPhase.INITIALIZING
            self.stats.total_tables = len(table_names)
            
            # Initialize table progress
            for table_name in table_names:
                self.table_progress[table_name] = TableProgress(name=table_name)
            
            self._phase_start_times[MigrationPhase.INITIALIZING] = datetime.now()
    
    def set_phase(self, phase: MigrationPhase) -> None:
        """
        Set current migration phase.
        
        Args:
            phase: New migration phase
        """
        with self.lock:
            old_phase = self.stats.phase
            self.stats.phase = phase
            self._phase_start_times[phase] = datetime.now()
            
            print(f"🔄 Phase changed: {old_phase.value} → {phase.value}")
    
    def start_table(self, table_name: str, total_records: int) -> None:
        """
        Start monitoring a table migration.
        
        Args:
            table_name: Name of the table
            total_records: Total number of records to migrate
        """
        with self.lock:
            if table_name not in self.table_progress:
                self.table_progress[table_name] = TableProgress(name=table_name)
            
            progress = self.table_progress[table_name]
            progress.status = TableStatus.IN_PROGRESS
            progress.total_records = total_records
            progress.start_time = datetime.now()
            progress.processed_records = 0
            
            # Update overall stats
            self.stats.total_records += total_records
            
            print(f"📊 Starting table '{table_name}' ({total_records:,} records)")
    
    def update_table_progress(self, table_name: str, processed_records: int, 
                            current_batch: int = 0, batch_count: int = 0) -> None:
        """
        Update progress for a table.
        
        Args:
            table_name: Name of the table
            processed_records: Number of records processed
            current_batch: Current batch number
            batch_count: Total number of batches
        """
        with self.lock:
            if table_name not in self.table_progress:
                return
            
            progress = self.table_progress[table_name]
            old_processed = progress.processed_records
            progress.processed_records = processed_records
            progress.current_batch = current_batch
            progress.batch_count = batch_count
            
            # Update overall stats
            self.stats.processed_records += (processed_records - old_processed)
            
            self._last_update = datetime.now()
    
    def complete_table(self, table_name: str, success: bool = True, 
                      error_message: Optional[str] = None) -> None:
        """
        Mark a table as completed.
        
        Args:
            table_name: Name of the table
            success: Whether migration was successful
            error_message: Error message if failed
        """
        with self.lock:
            if table_name not in self.table_progress:
                return
            
            progress = self.table_progress[table_name]
            progress.end_time = datetime.now()
            progress.error_message = error_message
            
            if success:
                progress.status = TableStatus.COMPLETED
                self.stats.completed_tables += 1
                status_icon = "✅"
            else:
                progress.status = TableStatus.FAILED
                self.stats.failed_tables += 1
                status_icon = "❌"
            
            duration = progress.duration
            rate = progress.records_per_second
            
            print(f"{status_icon} Table '{table_name}' completed "
                  f"({progress.processed_records:,} records, "
                  f"{duration.total_seconds():.1f}s, {rate:.1f} rec/s)")
            
            if error_message:
                print(f"   Error: {error_message}")
    
    def skip_table(self, table_name: str, reason: str) -> None:
        """
        Mark a table as skipped.
        
        Args:
            table_name: Name of the table
            reason: Reason for skipping
        """
        with self.lock:
            if table_name not in self.table_progress:
                self.table_progress[table_name] = TableProgress(name=table_name)
            
            progress = self.table_progress[table_name]
            progress.status = TableStatus.SKIPPED
            progress.error_message = reason
            self.stats.skipped_tables += 1
            
            print(f"⏭️  Skipped table '{table_name}': {reason}")
    
    def complete_migration(self, success: bool = True) -> None:
        """
        Mark migration as completed.
        
        Args:
            success: Whether migration was successful overall
        """
        with self.lock:
            self.stats.end_time = datetime.now()
            self.stats.phase = MigrationPhase.COMPLETED if success else MigrationPhase.FAILED
            
            self.print_final_summary()
    
    def add_error(self) -> None:
        """Increment error count."""
        with self.lock:
            self.stats.errors_count += 1
    
    def add_warning(self) -> None:
        """Increment warning count."""
        with self.lock:
            self.stats.warnings_count += 1
    
    def get_status_summary(self) -> Dict[str, Any]:
        """
        Get current status summary.
        
        Returns:
            Dictionary containing current status information
        """
        with self.lock:
            # Get table status counts
            status_counts = {}
            for status in TableStatus:
                status_counts[status.value] = sum(
                    1 for p in self.table_progress.values() 
                    if p.status == status
                )
            
            return {
                'phase': self.stats.phase.value,
                'duration': self.stats.duration.total_seconds() if self.stats.duration else 0,
                'overall_progress': self.stats.overall_progress,
                'table_progress': self.stats.table_progress,
                'total_tables': self.stats.total_tables,
                'completed_tables': self.stats.completed_tables,
                'failed_tables': self.stats.failed_tables,
                'skipped_tables': self.stats.skipped_tables,
                'total_records': self.stats.total_records,
                'processed_records': self.stats.processed_records,
                'records_per_second': self.stats.records_per_second,
                'errors_count': self.stats.errors_count,
                'warnings_count': self.stats.warnings_count,
                'table_status_counts': status_counts,
                'active_tables': [
                    {
                        'name': p.name,
                        'progress': p.progress_percentage,
                        'records': f"{p.processed_records}/{p.total_records}",
                        'rate': p.records_per_second
                    }
                    for p in self.table_progress.values()
                    if p.status == TableStatus.IN_PROGRESS
                ]
            }
    
    def print_progress_update(self) -> None:
        """Print a progress update to console."""
        with self.lock:
            if datetime.now() - self._last_update < timedelta(seconds=5):
                return  # Don't update too frequently
            
            print(f"\n📈 Progress Update - {self.stats.phase.value}")
            print(f"   Overall: {self.stats.overall_progress:.1f}% "
                  f"({self.stats.processed_records:,}/{self.stats.total_records:,} records)")
            print(f"   Tables: {self.stats.completed_tables}/{self.stats.total_tables} completed")
            
            if self.stats.records_per_second > 0:
                print(f"   Rate: {self.stats.records_per_second:.1f} records/sec")
            
            # Show active tables
            active_tables = [
                p for p in self.table_progress.values() 
                if p.status == TableStatus.IN_PROGRESS
            ]
            
            if active_tables:
                print("   Active:")
                for table in active_tables[:3]:  # Show up to 3 active tables
                    print(f"     • {table.name}: {table.progress_percentage:.1f}% "
                          f"({table.processed_records:,}/{table.total_records:,})")
    
    def print_final_summary(self) -> None:
        """Print final migration summary."""
        print("\n" + "=" * 60)
        print("📊 MIGRATION SUMMARY")
        print("=" * 60)
        
        # Overall stats
        duration = self.stats.duration
        if duration:
            hours, remainder = divmod(duration.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            duration_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
            print(f"Duration: {duration_str}")
        
        print(f"Phase: {self.stats.phase.value}")
        print(f"Total Records: {self.stats.processed_records:,}")
        
        if self.stats.records_per_second > 0:
            print(f"Average Rate: {self.stats.records_per_second:.1f} records/sec")
        
        print()
        
        # Table summary
        print("📋 Table Summary:")
        print(f"  ✅ Completed: {self.stats.completed_tables}")
        print(f"  ❌ Failed: {self.stats.failed_tables}")
        print(f"  ⏭️  Skipped: {self.stats.skipped_tables}")
        print(f"  📊 Total: {self.stats.total_tables}")
        
        if self.stats.errors_count > 0 or self.stats.warnings_count > 0:
            print()
            print("⚠️  Issues:")
            print(f"  Errors: {self.stats.errors_count}")
            print(f"  Warnings: {self.stats.warnings_count}")
        
        print("=" * 60)
    
    def get_table_details(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table details or None if not found
        """
        with self.lock:
            if table_name not in self.table_progress:
                return None
            
            progress = self.table_progress[table_name]
            return {
                'name': progress.name,
                'status': progress.status.value,
                'total_records': progress.total_records,
                'processed_records': progress.processed_records,
                'progress_percentage': progress.progress_percentage,
                'duration': progress.duration.total_seconds() if progress.duration else 0,
                'records_per_second': progress.records_per_second,
                'batch_info': {
                    'current_batch': progress.current_batch,
                    'total_batches': progress.batch_count
                } if progress.batch_count > 0 else None,
                'error_message': progress.error_message
            }
    
    def reset(self) -> None:
        """Reset all monitoring data."""
        with self.lock:
            self.stats = MigrationStats()
            self.table_progress.clear()
            self._phase_start_times.clear()
            self._last_update = datetime.now()
