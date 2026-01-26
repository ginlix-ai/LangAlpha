"""
Database utilities for PostgreSQL operations.

Provides common patterns for building dynamic queries.
"""

from typing import Any, List, Optional, Tuple

from psycopg.types.json import Json


class UpdateQueryBuilder:
    """
    Builder for dynamic UPDATE queries with optional field updates.

    Supports:
    - Optional field updates (only non-None values are included)
    - JSONB fields with automatic Json() wrapping
    - Automatic updated_at timestamp

    Usage:
        builder = UpdateQueryBuilder()
        builder.add_field("name", name)
        builder.add_field("metadata", metadata, is_json=True)

        if not builder.has_updates():
            return await get_current_record(...)

        query, params = builder.build(
            table="users",
            where_clause="user_id = %s",
            where_params=[user_id],
            returning_columns=["user_id", "name", "created_at", "updated_at"],
        )
    """

    def __init__(self):
        self._updates: List[str] = []
        self._params: List[Any] = []

    def add_field(
        self,
        column: str,
        value: Any,
        *,
        is_json: bool = False,
    ) -> "UpdateQueryBuilder":
        """
        Add a field to update if value is not None.

        Args:
            column: Database column name
            value: Value to set (skipped if None)
            is_json: If True, wrap value with Json() for JSONB columns

        Returns:
            Self for method chaining
        """
        if value is not None:
            self._updates.append(f"{column} = %s")
            self._params.append(Json(value) if is_json else value)
        return self

    def has_updates(self) -> bool:
        """Check if any fields were added for update."""
        return len(self._updates) > 0

    def build(
        self,
        table: str,
        where_clause: str,
        where_params: List[Any],
        returning_columns: Optional[List[str]] = None,
        *,
        include_updated_at: bool = True,
    ) -> Tuple[str, Tuple[Any, ...]]:
        """
        Build the UPDATE query.

        Args:
            table: Table name
            where_clause: WHERE clause (without WHERE keyword)
            where_params: Parameters for the WHERE clause
            returning_columns: Columns to return (optional)
            include_updated_at: If True, add "updated_at = NOW()"

        Returns:
            Tuple of (query_string, parameters_tuple)

        Raises:
            ValueError: If no fields were added for update
        """
        if not self._updates:
            raise ValueError("No fields to update")

        updates = self._updates.copy()
        params = self._params.copy()

        if include_updated_at:
            updates.append("updated_at = NOW()")

        params.extend(where_params)

        query = f"UPDATE {table} SET {', '.join(updates)} WHERE {where_clause}"

        if returning_columns:
            query += f" RETURNING {', '.join(returning_columns)}"

        return query, tuple(params)
