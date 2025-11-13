import streamlit as st
import pandas as pd
import altair as alt
from db import get_connection


@st.cache_data(ttl=60)
def get_df(sql: str, params: tuple | list | None = None) -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        return df
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_workspaces() -> pd.DataFrame:
    return get_df(
        """
        SELECT id, name
        FROM public.workspaces
        WHERE deleted_at IS NULL
        ORDER BY name
        """
    )


@st.cache_data(ttl=300)
def load_projects(workspace_id: str) -> pd.DataFrame:
    return get_df(
        """
        SELECT id, name
        FROM public.projects
        WHERE workspace_id = %s AND deleted_at IS NULL
        ORDER BY name
        """,
        (workspace_id,),
    )


@st.cache_data(ttl=300)
def load_cycles(project_id: str) -> pd.DataFrame:
    return get_df(
        """
        SELECT id, name, start_date, end_date
        FROM public.cycles
        WHERE project_id = %s AND deleted_at IS NULL
        ORDER BY COALESCE(start_date, 'epoch'::timestamptz), name
        """,
        (project_id,),
    )


@st.cache_data(ttl=300)
def load_states(project_id: str) -> pd.DataFrame:
    return get_df(
        """
        SELECT id, name, "group"
        FROM public.states
        WHERE project_id = %s AND deleted_at IS NULL
        ORDER BY name
        """,
        (project_id,),
    )


@st.cache_data(ttl=300)
def load_labels(project_id: str) -> pd.DataFrame:
    return get_df(
        """
        SELECT id, name
        FROM public.labels
        WHERE (project_id = %s OR project_id IS NULL) AND deleted_at IS NULL
        ORDER BY name
        """,
        (project_id,),
    )


@st.cache_data(ttl=300)
def load_workspace_users(workspace_id: str) -> pd.DataFrame:
    # Usuários membros do workspace
    return get_df(
        """
        SELECT u.id, COALESCE(u.display_name, u.username) AS name
        FROM public.users u
        WHERE u.is_active = TRUE
          AND EXISTS (
            SELECT 1 FROM public.workspace_members wm
            WHERE wm.workspace_id = %s 
              AND wm.member_id = u.id 
              AND wm.is_active = TRUE
              AND wm.deleted_at IS NULL
          )
        ORDER BY name
        """,
        (workspace_id,),
    )


def compute_sprint_metrics(
    cycle_ids: list[str],
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> pd.DataFrame:
    if not cycle_ids:
        return pd.DataFrame(columns=[
            "cycle_id", "cycle_name", "estimadas", "entregues", "pontos_estimados", "pontos_entregues"
        ])

    # Placeholders para IN
    cycle_ph = ",".join(["%s"] * len(cycle_ids))

    filters_sql = [
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
        "ci.project_id = %s",
    ]
    params: list = [*cycle_ids, project_id]

    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_assignees ia WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL AND ia.assignee_id IN ({ass_ph}))"
        )
        params.extend(assignee_ids)

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        filters_sql.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    sql = f"""
        SELECT
            ci.cycle_id,
            c.name AS cycle_name,
            c.start_date,
            COUNT(DISTINCT ci.issue_id) AS estimadas,
            COUNT(DISTINCT CASE WHEN i.completed_at IS NOT NULL
                  AND (c.start_date IS NULL OR i.completed_at >= c.start_date)
                  AND (c.end_date   IS NULL OR i.completed_at <= c.end_date)
                THEN ci.issue_id END) AS entregues,
            COALESCE(SUM(COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int, 0)), 0) AS pontos_estimados,
            COALESCE(SUM(CASE WHEN i.completed_at IS NOT NULL
                  AND (c.start_date IS NULL OR i.completed_at >= c.start_date)
                  AND (c.end_date   IS NULL OR i.completed_at <= c.end_date)
                THEN COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int, 0) ELSE 0 END), 0) AS pontos_entregues
        FROM public.cycle_issues ci
        JOIN public.cycles c  ON c.id = ci.cycle_id
        JOIN public.issues i  ON i.id = ci.issue_id
        LEFT JOIN public.estimate_points ep ON ep.id = i.estimate_point_id
        WHERE ci.cycle_id IN ({cycle_ph})
          AND {' AND '.join(filters_sql)}
        GROUP BY ci.cycle_id, c.name, c.start_date
        ORDER BY c.start_date NULLS FIRST, c.name
    """
    return get_df(sql, tuple(params))


@st.cache_data(ttl=120)
def compute_rolled_tasks_count(
    cycle_ids: list[str],
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> int:
    if not cycle_ids:
        return 0

    cycle_ph = ",".join(["%s"] * len(cycle_ids))

    filters_sql = [
        f"ci.cycle_id IN ({cycle_ph})",
        "ci.project_id = %s",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
        "c.start_date IS NOT NULL",
    ]
    params: list = [*cycle_ids, project_id]

    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_assignees ia WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL AND ia.assignee_id IN ({ass_ph}))"
        )
        params.extend(assignee_ids)

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        filters_sql.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    sql = f"""
        WITH base AS (
            SELECT ci.issue_id, c.start_date, c.end_date, i.completed_at
            FROM public.cycle_issues ci
            JOIN public.cycles c ON c.id = ci.cycle_id
            JOIN public.issues i ON i.id = ci.issue_id
            WHERE {' AND '.join(filters_sql)}
        )
        SELECT COUNT(DISTINCT b.issue_id) AS rolled_count
        FROM base b
        WHERE (
            b.completed_at IS NULL
            OR (b.start_date IS NOT NULL AND b.completed_at < b.start_date)
            OR (b.end_date   IS NOT NULL AND b.completed_at > b.end_date)
        )
        AND EXISTS (
            SELECT 1
            FROM public.cycle_issues ci2
            JOIN public.cycles c2 ON c2.id = ci2.cycle_id
            WHERE ci2.issue_id = b.issue_id
              AND ci2.project_id = %s
              AND ci2.deleted_at IS NULL AND c2.deleted_at IS NULL
              AND c2.start_date IS NOT NULL
              AND c2.start_date > b.start_date
        )
    """
    df = get_df(sql, tuple(params + [project_id]))
    try:
        return int(df.iloc[0]["rolled_count"]) if not df.empty else 0
    except Exception:
        return 0


@st.cache_data(ttl=120)
def compute_productivity_avg_per_member(
    cycle_ids: list[str],
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> float:
    if not cycle_ids:
        return 0.0

    cycle_ph = ",".join(["%s"] * len(cycle_ids))
    params: list = [*cycle_ids, project_id]

    delivered_filters = [
        f"ci.cycle_id IN ({cycle_ph})",
        "ci.project_id = %s",
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
        "i.completed_at IS NOT NULL",
        "(c.start_date IS NULL OR i.completed_at >= c.start_date)",
        "(c.end_date   IS NULL OR i.completed_at <= c.end_date)",
    ]

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        delivered_filters.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        delivered_filters.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    # Assignee filter será aplicado na etapa de agrupamento
    assignee_filter_sql = ""
    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        assignee_filter_sql = f"AND ia.assignee_id IN ({ass_ph})"
        params.extend(assignee_ids)

    sql = f"""
        WITH delivered AS (
            SELECT DISTINCT i.id AS issue_id
            FROM public.cycle_issues ci
            JOIN public.cycles c ON c.id = ci.cycle_id
            JOIN public.issues i ON i.id = ci.issue_id
            WHERE {' AND '.join(delivered_filters)}
        )
        SELECT COALESCE(AVG(cnt), 0) AS avg_tasks
        FROM (
            SELECT ia.assignee_id, COUNT(DISTINCT ia.issue_id) AS cnt
            FROM public.issue_assignees ia
            JOIN delivered d ON d.issue_id = ia.issue_id
            WHERE ia.deleted_at IS NULL {assignee_filter_sql}
            GROUP BY ia.assignee_id
        ) s
    """
    df = get_df(sql, tuple(params))
    try:
        val = float(df.iloc[0]["avg_tasks"]) if not df.empty else 0.0
        return val
    except Exception:
        return 0.0


@st.cache_data(ttl=120)
def compute_points_avg_per_member(
    cycle_ids: list[str],
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> float:
    if not cycle_ids:
        return 0.0

    cycle_ph = ",".join(["%s"] * len(cycle_ids))
    params: list = [*cycle_ids, project_id]

    delivered_filters = [
        f"ci.cycle_id IN ({cycle_ph})",
        "ci.project_id = %s",
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
        "i.completed_at IS NOT NULL",
        "(c.start_date IS NULL OR i.completed_at >= c.start_date)",
        "(c.end_date   IS NULL OR i.completed_at <= c.end_date)",
    ]

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        delivered_filters.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        delivered_filters.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    assignee_filter_sql = ""
    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        assignee_filter_sql = f"AND ia.assignee_id IN ({ass_ph})"
        params.extend(assignee_ids)

    sql = f"""
        WITH delivered AS (
            SELECT DISTINCT i.id AS issue_id
            FROM public.cycle_issues ci
            JOIN public.cycles c ON c.id = ci.cycle_id
            JOIN public.issues i ON i.id = ci.issue_id
            WHERE {' AND '.join(delivered_filters)}
        )
        SELECT COALESCE(AVG(points_sum), 0) AS avg_points
        FROM (
            SELECT ia.assignee_id, SUM(COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int, 0)) AS points_sum
            FROM public.issue_assignees ia
            JOIN delivered d ON d.issue_id = ia.issue_id
            JOIN public.issues i ON i.id = ia.issue_id
            LEFT JOIN public.estimate_points ep ON ep.id = i.estimate_point_id
            WHERE ia.deleted_at IS NULL {assignee_filter_sql}
            GROUP BY ia.assignee_id
        ) s
    """
    df = get_df(sql, tuple(params))
    try:
        val = float(df.iloc[0]["avg_points"]) if not df.empty else 0.0
        return val
    except Exception:
        return 0.0

@st.cache_data(ttl=120)
def compute_time_metrics_for_cycles(
    cycle_ids: list[str],
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> tuple[float, float]:
    """Retorna médias (em dias) de Lead Time e Cycle Time para issues concluídas nas sprints selecionadas.

    Lead Time: completed_at - created_on (proxy: MIN(issue_versions.created_at)).
    Cycle Time: completed_at - start_on (proxy: MIN(issue_versions.start_date), fallback: cycle_issues.created_at).
    """
    if not cycle_ids:
        return 0.0, 0.0

    cycle_ph = ",".join(["%s"] * len(cycle_ids))
    params: list = [*cycle_ids, project_id]

    base_filters = [
        f"ci.cycle_id IN ({cycle_ph})",
        "ci.project_id = %s",
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
    ]

    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        base_filters.append(
            f"EXISTS (SELECT 1 FROM public.issue_assignees ia WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL AND ia.assignee_id IN ({ass_ph}))"
        )
        params.extend(assignee_ids)

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        base_filters.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        base_filters.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    sql = f"""
        WITH base AS (
            SELECT ci.cycle_id,
                   c.name AS cycle_name,
                   c.start_date,
                   c.end_date,
                   ci.created_at AS committed_at,
                   i.id AS issue_id,
                   i.completed_at,
                   i.created_at AS issue_created_at,
                   (
                     SELECT MIN(iv.start_date)
                     FROM public.issue_versions iv
                     WHERE iv.issue_id = i.id AND iv.deleted_at IS NULL
                   ) AS iv_started_on,
                   i.start_date AS issue_start_date
            FROM public.cycle_issues ci
            JOIN public.cycles c ON c.id = ci.cycle_id
            JOIN public.issues i ON i.id = ci.issue_id
            WHERE {' AND '.join(base_filters)}
        )
        SELECT
           COALESCE(AVG((b.completed_at::date - b.issue_created_at::date)), 0) AS lead_days_avg,
           COALESCE(AVG((b.completed_at::date - COALESCE(b.iv_started_on, b.issue_start_date, b.committed_at::date))), 0) AS cycle_days_avg
        FROM base b
        WHERE b.completed_at IS NOT NULL
          AND (b.start_date IS NULL OR b.completed_at >= b.start_date)
          AND (b.end_date   IS NULL OR b.completed_at <= b.end_date)
    """

    try:
        df = get_df(sql, tuple(params))
    except Exception:
        return 0.0, 0.0
    try:
        if df.empty:
            return 0.0, 0.0
        lead = float(df.iloc[0]["lead_days_avg"]) if df.iloc[0]["lead_days_avg"] is not None else 0.0
        cycle = float(df.iloc[0]["cycle_days_avg"]) if df.iloc[0]["cycle_days_avg"] is not None else 0.0
        return lead, cycle
    except Exception:
        return 0.0, 0.0


@st.cache_data(ttl=120)
def compute_member_metrics_for_cycles(
    cycle_ids: list[str],
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> pd.DataFrame:
    if not cycle_ids:
        return pd.DataFrame(columns=[
            "Dev", "Realizado", "Pontos realizados", "Pontos médios por issue",
            "Lead Time médio (dias)", "Cycle Time médio (dias)"
        ])

    cycle_ph = ",".join(["%s"] * len(cycle_ids))
    params: list = [*cycle_ids, project_id]

    base_filters = [
        f"ci.cycle_id IN ({cycle_ph})",
        "ci.project_id = %s",
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
    ]

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        base_filters.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        base_filters.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    assignee_filter_sql = ""
    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        assignee_filter_sql = f"AND ia.assignee_id IN ({ass_ph})"
        params.extend(assignee_ids)

    sql = f"""
        WITH base AS (
            SELECT ci.cycle_id,
                   c.name AS cycle_name,
                   c.start_date,
                   c.end_date,
                   ci.created_at::date AS committed_at,
                   i.id AS issue_id,
                   i.completed_at AS completed_at,
                   i.created_at::date AS issue_created_at,
                   (
                     SELECT MIN(iv.start_date)::date
                     FROM public.issue_versions iv
                     WHERE iv.issue_id = i.id AND iv.deleted_at IS NULL
                   ) AS iv_started_on,
                   i.start_date::date AS issue_start_date,
                   COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int, 0) AS point
            FROM public.cycle_issues ci
            JOIN public.cycles c ON c.id = ci.cycle_id
            JOIN public.issues i ON i.id = ci.issue_id
            LEFT JOIN public.estimate_points ep ON ep.id = i.estimate_point_id
            WHERE {' AND '.join(base_filters)}
        ), delivered AS (
            SELECT b.*, ia.assignee_id, COALESCE(u.display_name, u.username) AS dev_name
            FROM base b
            JOIN public.issue_assignees ia ON ia.issue_id = b.issue_id AND ia.deleted_at IS NULL {assignee_filter_sql}
            LEFT JOIN public.users u ON u.id = ia.assignee_id
        )
        SELECT
            d.dev_name AS "Dev",
            COUNT(DISTINCT d.issue_id) FILTER (
                WHERE d.completed_at IS NOT NULL
                  AND (d.start_date IS NULL OR d.completed_at >= d.start_date)
                  AND (d.end_date   IS NULL OR d.completed_at <= d.end_date)
            ) AS "Realizado",
            COALESCE(SUM(d.point) FILTER (
                WHERE d.completed_at IS NOT NULL
                  AND (d.start_date IS NULL OR d.completed_at >= d.start_date)
                  AND (d.end_date   IS NULL OR d.completed_at <= d.end_date)
            ), 0) AS "Pontos entregues",
            COALESCE(AVG(d.point::double precision) FILTER (
                WHERE d.completed_at IS NOT NULL
                  AND (d.start_date IS NULL OR d.completed_at >= d.start_date)
                  AND (d.end_date   IS NULL OR d.completed_at <= d.end_date)
            ), 0) AS "Pontos médios por issue",
            COALESCE(AVG(EXTRACT(EPOCH FROM (d.completed_at - d.issue_created_at)) / 86400.0) FILTER (
                WHERE d.completed_at IS NOT NULL
                  AND (d.start_date IS NULL OR d.completed_at >= d.start_date)
                  AND (d.end_date   IS NULL OR d.completed_at <= d.end_date)
            ), 0) AS "Lead Time médio (dias)",
            COALESCE(AVG(EXTRACT(EPOCH FROM (d.completed_at - COALESCE(d.iv_started_on, d.issue_start_date, d.committed_at)) ) / 86400.0) FILTER (
                WHERE d.completed_at IS NOT NULL
                  AND (d.start_date IS NULL OR d.completed_at >= d.start_date)
                  AND (d.end_date   IS NULL OR d.completed_at <= d.end_date)
            ), 0) AS "Cycle Time médio (dias)"
        FROM delivered d
        GROUP BY d.dev_name
        ORDER BY "Pontos entregues" DESC, "Realizado" DESC, d.dev_name
    """
    return get_df(sql, tuple(params))

@st.cache_data(ttl=120)
def load_issues_for_cycles(
    cycle_ids: list[str],
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> pd.DataFrame:
    if not cycle_ids:
        return pd.DataFrame(columns=["Sprint", "Issue", "Estado", "Criada em", "Iniciada em", "Concluída em", "Responsáveis"])

    cycle_ph = ",".join(["%s"] * len(cycle_ids))

    filters_sql = [
        f"ci.cycle_id IN ({cycle_ph})",
        "ci.project_id = %s",
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
    ]
    params: list = [*cycle_ids, project_id]

    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_assignees ia WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL AND ia.assignee_id IN ({ass_ph}))"
        )
        params.extend(assignee_ids)

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        filters_sql.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    sql = f"""
        SELECT
            c.name AS "Sprint",
            i.name AS "Issue",
            s.name AS "Estado",
            to_char(i.created_at, 'YYYY-MM-DD') AS "Criada em",
            CASE WHEN s."group" IN ('started', 'completed')
                 THEN to_char(
                    COALESCE(
                        (
                            SELECT MIN(ia.created_at)::date
                            FROM public.issue_activities ia
                            JOIN public.states ss ON ss.id = ia.new_identifier
                            WHERE ia.issue_id = i.id
                              AND ia.deleted_at IS NULL
                              AND ia.field = 'state'
                              AND ss."group" = 'started'
                        ),
                        (
                            SELECT MIN(iv.last_saved_at)::date
                            FROM public.issue_versions iv
                            JOIN public.states ss ON ss.id = iv.state
                            WHERE iv.issue_id = i.id
                              AND iv.deleted_at IS NULL
                              AND ss."group" = 'started'
                        ),
                        i.start_date::date
                    ), 'YYYY-MM-DD'
                 )
                 ELSE NULL
            END AS "Iniciada em",
            to_char(i.completed_at, 'YYYY-MM-DD') AS "Concluída em",
            COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) AS "Estimativa",
            i.priority AS "Prioridade",
            CONCAT_WS(', ',
                CASE WHEN COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) IS NULL OR COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) = 0
                     THEN '⚠️ Sem estimativa' ELSE NULL END,
                CASE WHEN TRIM(LOWER(i.priority)) IN ('', 'none', 'sem prioridade', 'nao definida', 'não definida', 'undefined')
                     THEN '⚠️ Sem prioridade' ELSE NULL END
            ) AS "Alertas",
            CASE
                WHEN i.completed_at IS NOT NULL
                 AND (c.start_date IS NULL OR i.completed_at >= c.start_date)
                 AND (c.end_date   IS NULL OR i.completed_at <= c.end_date)
                THEN 'Entregue' ELSE 'Não entregue'
            END AS "Entrega",
            (
                SELECT STRING_AGG(COALESCE(u.display_name, u.username), ', ')
                FROM public.issue_assignees ia
                JOIN public.users u ON u.id = ia.assignee_id
                WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL
            ) AS "Responsáveis"
        FROM public.cycle_issues ci
        JOIN public.cycles c ON c.id = ci.cycle_id
        JOIN public.issues i ON i.id = ci.issue_id
        LEFT JOIN public.states s ON s.id = i.state_id
        LEFT JOIN public.estimate_points ep ON ep.id = i.estimate_point_id
        WHERE {' AND '.join(filters_sql)}
        ORDER BY c.start_date NULLS FIRST, c.name, i.id
    """
    return get_df(sql, tuple(params))

@st.cache_data(ttl=120)
def load_issues_for_current_sprint(
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> pd.DataFrame:
    filters_sql = [
        "ci.project_id = %s",
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
        "c.start_date IS NOT NULL",
        "c.start_date <= NOW()",
        "(c.end_date IS NULL OR c.end_date >= NOW())",
    ]
    params: list = [project_id]

    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_assignees ia WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL AND ia.assignee_id IN ({ass_ph}))"
        )
        params.extend(assignee_ids)

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        filters_sql.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    sql = f"""
        SELECT
            c.name AS "Sprint",
            i.name AS "Issue",
            s.name AS "Estado",
            to_char(i.created_at, 'YYYY-MM-DD') AS "Criada em",
            CASE WHEN s."group" IN ('started', 'completed')
                 THEN to_char(
                    COALESCE(
                        (
                            SELECT MIN(ia.created_at)::date
                            FROM public.issue_activities ia
                            JOIN public.states ss ON ss.id = ia.new_identifier
                            WHERE ia.issue_id = i.id
                              AND ia.deleted_at IS NULL
                              AND ia.field = 'state'
                              AND ss."group" = 'started'
                        ),
                        (
                            SELECT MIN(iv.last_saved_at)::date
                            FROM public.issue_versions iv
                            JOIN public.states ss ON ss.id = iv.state
                            WHERE iv.issue_id = i.id
                              AND iv.deleted_at IS NULL
                              AND ss."group" = 'started'
                        ),
                        i.start_date::date
                    ), 'YYYY-MM-DD'
                 )
                 ELSE NULL
            END AS "Iniciada em",
            to_char(i.completed_at, 'YYYY-MM-DD') AS "Concluída em",
            COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) AS "Estimativa",
            i.priority AS "Prioridade",
            CONCAT_WS(', ',
                CASE WHEN COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) IS NULL OR COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) = 0
                     THEN '⚠️ Sem estimativa' ELSE NULL END,
                CASE WHEN TRIM(LOWER(i.priority)) IN ('', 'none', 'sem prioridade', 'nao definida', 'não definida', 'undefined')
                     THEN '⚠️ Sem prioridade' ELSE NULL END
            ) AS "Alertas",
            CASE
                WHEN i.completed_at IS NOT NULL
                 AND (c.start_date IS NULL OR i.completed_at >= c.start_date)
                 AND (c.end_date   IS NULL OR i.completed_at <= c.end_date)
                THEN 'Entregue' ELSE 'Não entregue'
            END AS "Entrega",
            (
                SELECT STRING_AGG(COALESCE(u.display_name, u.username), ', ')
                FROM public.issue_assignees ia
                JOIN public.users u ON u.id = ia.assignee_id
                WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL
            ) AS "Responsáveis"
        FROM public.cycle_issues ci
        JOIN public.cycles c ON c.id = ci.cycle_id
        JOIN public.issues i ON i.id = ci.issue_id
        LEFT JOIN public.states s ON s.id = i.state_id
        LEFT JOIN public.estimate_points ep ON ep.id = i.estimate_point_id
        WHERE {' AND '.join(filters_sql)}
        ORDER BY c.start_date NULLS FIRST, c.name, i.id
    """
    return get_df(sql, tuple(params))

@st.cache_data(ttl=120)
def load_backlog_issues(
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> pd.DataFrame:
    filters_sql = [
        "i.project_id = %s",
        "i.deleted_at IS NULL",
        "s.\"group\" = 'backlog'",
    ]
    params: list = [project_id]

    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_assignees ia WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL AND ia.assignee_id IN ({ass_ph}))"
        )
        params.extend(assignee_ids)

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        filters_sql.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    sql = f"""
        SELECT
            i.name AS "Issue",
            s.name AS "Estado",
            to_char(i.created_at, 'YYYY-MM-DD') AS "Criada em",
            CASE WHEN s."group" = 'started'
                 THEN to_char(
                    COALESCE(
                        (
                            SELECT MIN(iv.last_saved_at)::date
                            FROM public.issue_versions iv
                            JOIN public.states ss ON ss.id = iv.state
                            WHERE iv.issue_id = i.id
                              AND iv.deleted_at IS NULL
                              AND ss."group" = 'started'
                        ),
                        i.start_date::date
                    ), 'YYYY-MM-DD'
                 )
                 ELSE NULL
            END AS "Iniciada em",
            to_char(i.completed_at, 'YYYY-MM-DD') AS "Concluída em",
            COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) AS "Estimativa",
            i.priority AS "Prioridade",
            CONCAT_WS(', ',
                CASE WHEN COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) IS NULL OR COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) = 0
                     THEN '⚠️ Sem estimativa' ELSE NULL END,
                CASE WHEN TRIM(LOWER(i.priority)) IN ('', 'none', 'sem prioridade', 'nao definida', 'não definida', 'undefined')
                     THEN '⚠️ Sem prioridade' ELSE NULL END
            ) AS "Alertas",
            (
                SELECT STRING_AGG(COALESCE(u.display_name, u.username), ', ')
                FROM public.issue_assignees ia
                JOIN public.users u ON u.id = ia.assignee_id
                WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL
            ) AS "Responsáveis"
        FROM public.issues i
        LEFT JOIN public.states s ON s.id = i.state_id
        LEFT JOIN public.estimate_points ep ON ep.id = i.estimate_point_id
        WHERE {' AND '.join(filters_sql)}
        ORDER BY i.id DESC
    """
    return get_df(sql, tuple(params))

@st.cache_data(ttl=120)
def compute_label_breakdown_for_cycles(
    cycle_ids: list[str],
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> pd.DataFrame:
    if not cycle_ids:
        return pd.DataFrame(columns=["Sprint", "LabelCat", "Previsto", "Realizado"])

    cycle_ph = ",".join(["%s"] * len(cycle_ids))

    filters_sql = [
        f"ci.cycle_id IN ({cycle_ph})",
        "ci.project_id = %s",
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
    ]
    params: list = [*cycle_ids, project_id]

    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_assignees ia WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL AND ia.assignee_id IN ({ass_ph}))"
        )
        params.extend(assignee_ids)

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        filters_sql.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    sql = f"""
        WITH base AS (
            SELECT ci.cycle_id,
                   c.name AS cycle_name,
                   c.start_date,
                   c.end_date,
                   ci.created_at AS ci_created_at,
                   i.id AS issue_id,
                   i.completed_at,
                   i.type_id,
                   LOWER(p.identifier) AS project_prefix
            FROM public.cycle_issues ci
            JOIN public.cycles c ON c.id = ci.cycle_id
            JOIN public.issues i ON i.id = ci.issue_id
            JOIN public.projects p ON p.id = ci.project_id
            WHERE {' AND '.join(filters_sql)}
        ), labels_map AS (
            SELECT il.issue_id,
                   STRING_AGG(LOWER(l.name), ',') AS labels
            FROM public.issue_labels il
            JOIN public.labels l ON l.id = il.label_id
            WHERE il.deleted_at IS NULL
            GROUP BY il.issue_id
        ), type_map AS (
            SELECT it.id AS type_id, LOWER(it.name) AS type_name
            FROM public.issue_types it
        ), classified AS (
            SELECT b.cycle_id,
                   b.cycle_name,
                   b.start_date,
                   b.end_date,
                   b.ci_created_at,
                   b.issue_id,
                   b.completed_at,
                    CASE
                      WHEN (
                            lm.labels ~* ('(^|[,\s\-_])' || b.project_prefix || '[-_]*(nao|não)[- _]*planejada([,\s\-_]|$)')
                            OR lm.labels ILIKE ('%%' || b.project_prefix || '%%unplanned%%')
                            OR lm.labels ~* '(nao|não)[\s\-_]*planejada'
                            OR lm.labels ILIKE '%%unplanned%%'
                          ) THEN 'Não planejada'
                      WHEN (
                            lm.labels ~* ('(^|[,\s\-_])' || b.project_prefix || '[-_]*bug[-_]*glpi([,\s\-_]|$)')
                            OR lm.labels ~* ('(^|[,\s\-_])' || b.project_prefix || '[-_]*glpi[-_]*bug([,\s\-_]|$)')
                            OR lm.labels ILIKE ('%%' || b.project_prefix || '%%bugglpi%%')
                            OR lm.labels ILIKE '%%bugglpi%%'
                            OR lm.labels ~* 'bug[\s\-_]*glpi'
                            OR lm.labels ~* 'glpi[\s\-_]*bug'
                          ) THEN 'Bug GLPI'
                      WHEN (
                            lm.labels ~* ('(^|[,\s\-_])' || b.project_prefix || '[-_]*bug([,\s\-_]|$)')
                            OR lm.labels ~* '(^|[,\s\-_])bug([,\s\-_]|$)'
                            OR tm.type_name = 'bug'
                          ) THEN 'Bug'
                      WHEN (
                            lm.labels ~* ('(^|[,\s\-_])' || b.project_prefix || '[-_]*feature([,\s\-_]|$)')
                            OR lm.labels ~* '(^|[,\s\-_])feature([,\s\-_]|$)'
                            OR tm.type_name = 'feature'
                          ) THEN 'Feature'
                      ELSE 'Outros'
                    END AS label_cat
            FROM base b
            LEFT JOIN labels_map lm ON lm.issue_id = b.issue_id
            LEFT JOIN type_map tm ON tm.type_id = b.type_id
        )
        SELECT c.cycle_id,
               c.cycle_name AS "Sprint",
               c.label_cat AS "LabelCat",
               COUNT(DISTINCT c.issue_id) AS "Previsto",
               COUNT(DISTINCT CASE
                                WHEN c.completed_at IS NOT NULL
                                 AND (c.start_date IS NULL OR c.completed_at >= c.start_date)
                                 AND (c.end_date IS NULL OR c.completed_at <= c.end_date)
                                THEN c.issue_id END) AS "Realizado"
        FROM classified c
        GROUP BY c.cycle_id, c.cycle_name, c.label_cat
        ORDER BY c.cycle_id, c.cycle_name, c.label_cat
    """

    df = get_df(sql, tuple(params))
    return df

@st.cache_data(ttl=120)
def compute_alerts_counts_for_current_sprint(
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> int:
    filters_sql = [
        "ci.project_id = %s",
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
        "c.start_date IS NOT NULL",
        "c.start_date <= NOW()",
        "(c.end_date IS NULL OR c.end_date >= NOW())",
        "(COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) IS NULL OR COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) = 0"
        " OR TRIM(LOWER(i.priority)) IN ('', 'none', 'sem prioridade', 'nao definida', 'não definida', 'undefined'))",
    ]
    params: list = [project_id]

    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_assignees ia WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL AND ia.assignee_id IN ({ass_ph}))"
        )
        params.extend(assignee_ids)

    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)

    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        filters_sql.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)

    sql = f"""
        SELECT COUNT(DISTINCT i.id) AS alerts_count
        FROM public.cycle_issues ci
        JOIN public.cycles c ON c.id = ci.cycle_id
        JOIN public.issues i ON i.id = ci.issue_id
        LEFT JOIN public.estimate_points ep ON ep.id = i.estimate_point_id
        WHERE {' AND '.join(filters_sql)}
    """
    df = get_df(sql, tuple(params))
    try:
        return int(df.iloc[0]["alerts_count"]) if not df.empty else 0
    except Exception:
        return 0

@st.cache_data(ttl=120)
def compute_alerts_counts_for_cycles(
    cycle_ids: list[str],
    project_id: str,
    assignee_ids: list[str] | None = None,
    label_ids: list[str] | None = None,
    state_ids: list[str] | None = None,
) -> int:
    if not cycle_ids:
        return 0
    cycle_ph = ",".join(["%s"] * len(cycle_ids))
    filters_sql = [
        f"ci.cycle_id IN ({cycle_ph})",
        "ci.project_id = %s",
        "ci.deleted_at IS NULL",
        "c.deleted_at IS NULL",
        "i.deleted_at IS NULL",
        "(COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) IS NULL OR COALESCE(i.point, ep.key, NULLIF(ep.value, '')::int) = 0"
        " OR TRIM(LOWER(i.priority)) IN ('', 'none', 'sem prioridade', 'nao definida', 'não definida', 'undefined'))",
    ]
    params: list = [*cycle_ids, project_id]
    if assignee_ids:
        ass_ph = ",".join(["%s"] * len(assignee_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_assignees ia WHERE ia.issue_id = i.id AND ia.deleted_at IS NULL AND ia.assignee_id IN ({ass_ph}))"
        )
        params.extend(assignee_ids)
    if label_ids:
        lab_ph = ",".join(["%s"] * len(label_ids))
        filters_sql.append(
            f"EXISTS (SELECT 1 FROM public.issue_labels il WHERE il.issue_id = i.id AND il.deleted_at IS NULL AND il.label_id IN ({lab_ph}))"
        )
        params.extend(label_ids)
    if state_ids:
        st_ph = ",".join(["%s"] * len(state_ids))
        filters_sql.append(f"i.state_id IN ({st_ph})")
        params.extend(state_ids)
    sql = f"""
        SELECT COUNT(DISTINCT i.id) AS alerts_count
        FROM public.cycle_issues ci
        JOIN public.cycles c ON c.id = ci.cycle_id
        JOIN public.issues i ON i.id = ci.issue_id
        LEFT JOIN public.estimate_points ep ON ep.id = i.estimate_point_id
        WHERE {' AND '.join(filters_sql)}
    """
    df = get_df(sql, tuple(params))
    try:
        return int(df.iloc[0]["alerts_count"]) if not df.empty else 0
    except Exception:
        return 0

def kpi_card(label: str, value: float | int, help: str | None = None, danger: bool = False):
    if danger:
        v = int(value) if isinstance(value, (int, float)) else value
        st.markdown(
            f"""
            <div style='width:100%;padding:16px;border-radius:8px;background:#ff4d4f;border:1px solid #a8071a;'>
              <div style='font-weight:700;color:#ffffff;'>⚠️ {label}</div>
              <div style='font-size:26px;font-weight:800;color:#ffffff;margin-top:6px;'>{v}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.metric(label=label, value=int(value) if isinstance(value, (int, float)) else value, help=help)


def main():
    st.set_page_config(page_title="Plane Dashboard", layout="wide")
    st.title("Dashboard de Sprints ")
    st.caption("Cartões de KPI, filtros avançados e gráfico Previsto vs Realizado")

    # Sidebar: filtros
    st.sidebar.header("Filtros avançados")

    workspaces = load_workspaces()
    if workspaces.empty:
        st.warning("Nenhum workspace encontrado.")
        return

    ws_name_to_id = {row["name"]: row["id"] for _, row in workspaces.iterrows()}
    ws_name = st.sidebar.selectbox("Workspace", list(ws_name_to_id.keys()))
    workspace_id = ws_name_to_id[ws_name]

    projects = load_projects(workspace_id)
    if projects.empty:
        st.warning("Nenhum projeto encontrado para o workspace selecionado.")
        return

    prj_name_to_id = {row["name"]: row["id"] for _, row in projects.iterrows()}
    prj_name = st.sidebar.selectbox("Projeto", list(prj_name_to_id.keys()))
    project_id = prj_name_to_id[prj_name]

    cycles = load_cycles(project_id)
    if cycles.empty:
        st.info("Nenhum sprint (cycle) encontrado para o projeto.")
    cy_name_to_id = {row["name"]: row["id"] for _, row in cycles.iterrows()}
    cy_names = st.sidebar.multiselect("Sprints", list(cy_name_to_id.keys()))
    cycle_ids = [cy_name_to_id[name] for name in cy_names]

    # Seleção efetiva de sprints: as escolhidas ou fallback para últimas 3 por start_date
    effective_cycle_ids = cycle_ids
    if not effective_cycle_ids and not cycles.empty:
        try:
            cycles_sorted = cycles.dropna(subset=["start_date"]).sort_values("start_date", ascending=False)
            effective_cycle_ids = list(cycles_sorted["id"].head(3))
            if effective_cycle_ids:
                st.info("Usando as últimas 3 sprints por padrão.")
        except Exception:
            effective_cycle_ids = []

    # Filtros opcionais
    users_df = load_workspace_users(workspace_id)
    user_name_to_id = {row["name"]: row["id"] for _, row in users_df.iterrows()}
    assignees_sel = st.sidebar.multiselect("Responsáveis", list(user_name_to_id.keys()))
    assignee_ids = [user_name_to_id[n] for n in assignees_sel]

    labels_df = load_labels(project_id)
    label_name_to_id = {row["name"]: row["id"] for _, row in labels_df.iterrows()}
    labels_sel = st.sidebar.multiselect("Labels", list(label_name_to_id.keys()))
    label_ids = [label_name_to_id[n] for n in labels_sel]

    states_df = load_states(project_id)
    state_name_to_id = {row["name"]: row["id"] for _, row in states_df.iterrows()}
    states_sel = st.sidebar.multiselect("Estados", list(state_name_to_id.keys()))
    state_ids = [state_name_to_id[n] for n in states_sel]

    st.sidebar.caption("Os filtros são aplicados às métricas e gráfico.")

    # Métricas
    metrics_df = compute_sprint_metrics(
        cycle_ids=effective_cycle_ids,
        project_id=project_id,
        assignee_ids=assignee_ids or None,
        label_ids=label_ids or None,
        state_ids=state_ids or None,
    )

    st.subheader("KPIs de Produto")

    total_estimadas = int(metrics_df["estimadas"].sum()) if not metrics_df.empty else 0
    total_entregues = int(metrics_df["entregues"].sum()) if not metrics_df.empty else 0
    produtividade_media = compute_productivity_avg_per_member(
        cycle_ids=effective_cycle_ids,
        project_id=project_id,
        assignee_ids=assignee_ids or None,
        label_ids=label_ids or None,
        state_ids=state_ids or None,
    )
    pontos_media_por_dev = compute_points_avg_per_member(
        cycle_ids=effective_cycle_ids,
        project_id=project_id,
        assignee_ids=assignee_ids or None,
        label_ids=label_ids or None,
        state_ids=state_ids or None,
    )

    # ⏱ Lead/Cycle Time médios (dias)
    lead_days_avg, cycle_days_avg = compute_time_metrics_for_cycles(
        cycle_ids=effective_cycle_ids,
        project_id=project_id,
        assignee_ids=assignee_ids or None,
        label_ids=label_ids or None,
        state_ids=state_ids or None,
    )

    top1, top2, top3, top4 = st.columns(4)
    with top1:
        kpi_card("Produtividade por DEV", f"{produtividade_media:.1f}", help="Média de tasks entregues por membro")
    with top2:
        kpi_card("Média de pontos por DEV", f"{pontos_media_por_dev:.1f}", help="Média de story points entregues por membro")
    with top3:
        kpi_card("⏱ Lead Time médio (dias)", f"{lead_days_avg:.1f}", help="Tempo médio do pedido até a entrega")
    with top4:
        kpi_card("⏱ Cycle Time médio (dias)", f"{cycle_days_avg:.1f}", help="Tempo médio de execução (início até entrega)")

    alerts_count = compute_alerts_counts_for_cycles(
        effective_cycle_ids,
        project_id=project_id,
        assignee_ids=assignee_ids or None,
        label_ids=label_ids or None,
        state_ids=state_ids or None,
    )

    # KPIs operacionais (sprints e tarefas)
    col1, col2, col3, col4 = st.columns(4)
    if metrics_df.empty:
        with col1:
            kpi_card("Sprints selecionadas", len(effective_cycle_ids))
        with col2:
            kpi_card("Tarefas estimadas", 0)
        with col3:
            kpi_card("Tarefas entregues", 0)
        with col4:
            kpi_card("⚠️ Tarefas com alertas", alerts_count, help="Sem prioridade ou estimativa", danger=alerts_count > 0)
        st.info("Selecione pelo menos uma sprint para visualizar métricas e gráfico.")
        return

    with col1:
        kpi_card("Sprints selecionadas", len(effective_cycle_ids))
    with col2:
        kpi_card("Tarefas estimadas", total_estimadas)
    with col3:
        kpi_card("Tarefas entregues", total_entregues)
    with col4:
        kpi_card(" Tarefas com alertas", alerts_count, help="Sem prioridade ou estimativa", danger=alerts_count > 0)

    st.subheader("Comparativo: Previsto vs Realizado por Sprint")

    chart_df = metrics_df[["cycle_name", "estimadas", "entregues", "start_date"]].copy()
    chart_df = chart_df.rename(columns={
        "cycle_name": "Sprint",
        "estimadas": "Previsto",
        "entregues": "Realizado",
        "start_date": "start_date",
    })
    chart_df["Sprint"] = chart_df["Sprint"].astype(str).str.strip()
    # Ordena pelo início da sprint para manter a leitura temporal
    chart_df = chart_df.sort_values("start_date", ascending=True)

    # Converte para formato longo para evitar ambiguidade de tipo em campos de transform
    long_df = pd.melt(
        chart_df,
        id_vars=["Sprint", "start_date"],
        value_vars=["Previsto", "Realizado"],
        var_name="Status",
        value_name="Quantidade",
    )
    # Reanexa colunas agregadas para tooltips
    long_df = long_df.merge(
        chart_df[["Sprint", "Previsto", "Realizado"]],
        on="Sprint",
        how="left",
    )
    # Calcula se entregues >= estimadas por sprint
    over_by_sprint = (
        chart_df.set_index("Sprint")["Realizado"] >= chart_df.set_index("Sprint")["Previsto"]
    )
    long_df["is_over"] = long_df["Sprint"].map(over_by_sprint)
    long_df["color_label"] = long_df.apply(
        lambda r: "Previsto" if r["Status"] == "Previsto" else ("Realizado ≥ Previsto" if r["is_over"] else "Realizado < Previsto"),
        axis=1,
    )

    bars = (
        alt.Chart(long_df)
        .mark_bar(size=42)
        .encode(
            x=alt.X(
                "Sprint:N",
                sort=None,
                axis=alt.Axis(labelAngle=0, tickBand='center', title='Sprint'),
                scale=alt.Scale(paddingInner=0.0, paddingOuter=0.12, align=0.5),
                bandPosition=0.5,
            ),
            xOffset=alt.XOffset(
                "Status:N",
                sort=["Previsto", "Realizado"],
                scale=alt.Scale(paddingInner=0.0)
            ),
            y=alt.Y("Quantidade:Q"),
            color=alt.Color(
                "color_label:N",
                scale=alt.Scale(
                    domain=["Previsto", "Realizado ≥ Previsto", "Realizado < Previsto"],
                    range=["#4e79a7", "#2ca02c", "#d62728"],
                ),
                legend=alt.Legend(title="Legenda"),
            ),
            tooltip=[
                alt.Tooltip("Sprint:N", title="Sprint"),
                alt.Tooltip("Quantidade:Q", title="Quantidade"),
                alt.Tooltip("Previsto:Q", title="Previsto"),
                alt.Tooltip("Realizado:Q", title="Realizado"),
            ],
        )
        .properties(height=380)
    )

    labels = (
        alt.Chart(long_df)
        .mark_text(dy=-6, color="#ffffff", fontSize=12, fontWeight="bold")
        .encode(
            x=alt.X(
                "Sprint:N",
                sort=None,
                axis=alt.Axis(labelAngle=0, tickBand='center', title='Sprint'),
                scale=alt.Scale(paddingInner=0.0, paddingOuter=0.12, align=0.5),
                bandPosition=0.5,
            ),
            xOffset=alt.XOffset(
                "Status:N",
                sort=["Previsto", "Realizado"],
                scale=alt.Scale(paddingInner=0.0)
            ),
            y=alt.Y("Quantidade:Q"),
            text=alt.Text("Quantidade:Q", format=".0f"),
        )
    )

    chart = (bars + labels).properties(padding={'left': 40, 'right': 12})
    st.altair_chart(chart, width="stretch")

    st.subheader("Comparativo por Tag: Feature vs Bug vs Bug GLPI vs Não planejada")
    if effective_cycle_ids:
        status_choice = st.radio(
            "Contagem",
            ["Previsto", "Realizado"],
            index=1,
            horizontal=True,
            key="labels_status_choice",
        )
        labels_df = compute_label_breakdown_for_cycles(
            effective_cycle_ids,
            project_id,
            assignee_ids or None,
            label_ids or None,
            state_ids or None,
        )
        if labels_df.empty:
            st.info("Nenhum dado de labels encontrado nas sprints selecionadas com os filtros atuais.")
        else:
            # Normaliza nomes de colunas vindos do banco, independente de casing
            labels_df_norm = labels_df.copy()
            labels_df_norm.rename(
                columns={
                    "label_cat": "LabelCat",
                    "estimadas": "Previsto",
                    "entregues": "Realizado",
                },
                inplace=True,
            )
            # Garanta nomes de sprint consistentes
            labels_df_norm["Sprint"] = labels_df_norm["Sprint"].astype(str).str.strip()

            # Totais por sprint (todas as categorias), para linha temporal
            totals_df = (
                labels_df_norm.groupby(["cycle_id", "Sprint"], as_index=False)[["Previsto", "Realizado"]].sum()
            )
            # Enrich com datas reais da sprint
            cycles_idx = cycles[["id", "start_date"]].rename(columns={"id": "cycle_id"})
            totals_df = totals_df.merge(cycles_idx, on="cycle_id", how="left")
            totals_df["start_date"] = pd.to_datetime(totals_df["start_date"], errors="coerce")
            # Ordenação por data de início da sprint
            order_df = totals_df.sort_values(["start_date", "Sprint"], ascending=[True, True])
            order_sprints = order_df["Sprint"].drop_duplicates().tolist()

            # Dados para gráfico de barras (apenas categorias desejadas)
            plot_df = labels_df_norm[labels_df_norm["LabelCat"].isin(["Feature", "Bug", "Bug GLPI", "Não planejada"])]
            status_map = {"Previsto": "Previsto", "Realizado": "Realizado"}
            status_col = status_map.get(status_choice, "Realizado")
            plot_df["Quantidade"] = plot_df[status_col]

            color_scale = alt.Scale(
                domain=["Feature", "Bug", "Bug GLPI", "Não planejada"],
                range=["#4e79a7", "#d62728", "#9467bd", "#ffbf00"],
            )

            chart_labels = (
                alt.Chart(plot_df)
                .mark_bar(size=14)
                .encode(
                    x=alt.X(
                        "Sprint:N",
                        sort=order_sprints,
                        axis=alt.Axis(labelAngle=0),
                        scale=alt.Scale(paddingInner=0.25, paddingOuter=0.1),
                    ),
                    xOffset=alt.XOffset("LabelCat:N", sort=["Feature", "Bug", "Bug GLPI", "Não planejada"]),
                    y=alt.Y("Quantidade:Q"),
                    color=alt.Color("LabelCat:N", scale=color_scale, legend=alt.Legend(title="Categoria")),
                    tooltip=[
                        alt.Tooltip("Sprint:N", title="Sprint"),
                        alt.Tooltip("LabelCat:N", title="Categoria"),
                        alt.Tooltip("Previsto:Q", title="Previsto"),
                        alt.Tooltip("Realizado:Q", title="Realizado"),
                    ],
                )
                .properties(height=320)
            )
            # Adiciona labels com as quantidades nas barras
            labels_text = (
                alt.Chart(plot_df)
                .mark_text(dy=-8, color="#ffffff", fontSize=10, fontWeight="bold")
                .encode(
                    x=alt.X(
                        "Sprint:N",
                        sort=order_sprints,
                        axis=alt.Axis(labelAngle=0),
                        scale=alt.Scale(paddingInner=0.25, paddingOuter=0.1),
                    ),
                    xOffset=alt.XOffset("LabelCat:N", sort=["Feature", "Bug", "Bug GLPI", "Não planejada"]),
                    y=alt.Y("Quantidade:Q"),
                    text=alt.Text("Quantidade:Q", format=".0f"),
                )
            )
            
            chart_with_labels = (chart_labels + labels_text)
            st.altair_chart(chart_with_labels, width="stretch")

            st.subheader("Linha temporal: Previsto vs Realizado por Sprint")
            est_peak_idx = totals_df["Previsto"].idxmax() if not totals_df.empty else None
            ent_peak_idx = totals_df["Realizado"].idxmax() if not totals_df.empty else None
            est_peak_df = totals_df.loc[[est_peak_idx]] if est_peak_idx is not None else totals_df.iloc[0:0]
            ent_peak_df = totals_df.loc[[ent_peak_idx]] if ent_peak_idx is not None else totals_df.iloc[0:0]

            est_line = (
                alt.Chart(totals_df)
                .mark_line(point=True, strokeWidth=3, color="#1f77b4")
                .encode(
                    x=alt.X("Sprint:N", sort=order_sprints, axis=alt.Axis(labelAngle=0, title="Sprint")),
                    y=alt.Y("Previsto:Q"),
                    tooltip=[
                        alt.Tooltip("Sprint:N", title="Sprint"),
                        alt.Tooltip("Previsto:Q", title="Previsto"),
                    ],
                )
                .properties(height=320)
            )

            ent_line = (
                alt.Chart(totals_df)
                .mark_line(point=True, strokeWidth=3, color="#ff7f0e")
                .encode(
                    x=alt.X("Sprint:N", sort=order_sprints, axis=alt.Axis(labelAngle=0, title="Sprint")),
                    y=alt.Y("Realizado:Q"),
                    tooltip=[
                        alt.Tooltip("Sprint:N", title="Sprint"),
                        alt.Tooltip("Realizado:Q", title="Realizado"),
                    ],
                )
            )

            legend_df = pd.DataFrame({"Legenda": ["Previsto", "Realizado"]})
            legend = (
                alt.Chart(legend_df)
                .mark_point()
                .encode(
                    color=alt.Color(
                        "Legenda:N",
                        scale=alt.Scale(domain=["Previsto", "Realizado"], range=["#1f77b4", "#ff7f0e"]),
                        legend=alt.Legend(title="Legenda"),
                    )
                )
            )

            est_peak = (
                alt.Chart(est_peak_df)
                .mark_point(size=150, filled=True, color="#1f77b4")
                .encode(
                    x=alt.X("Sprint:N", sort=order_sprints, axis=alt.Axis(labelAngle=0, title="Sprint")),
                    y=alt.Y("Previsto:Q"),
                    tooltip=[
                        alt.Tooltip("Sprint:N", title="Sprint"),
                        alt.Tooltip("Previsto:Q", title="Pico previsto"),
                    ],
                )
            )

            ent_peak = (
                alt.Chart(ent_peak_df)
                .mark_point(size=150, filled=True, color="#ff7f0e")
                .encode(
                    x=alt.X("Sprint:N", sort=order_sprints, axis=alt.Axis(labelAngle=0, title="Sprint")),
                    y=alt.Y("Realizado:Q"),
                    tooltip=[
                        alt.Tooltip("Sprint:N", title="Sprint"),
                        alt.Tooltip("Realizado:Q", title="Pico realizado"),
                    ],
                )
            )

            chart_time = est_line + ent_line + est_peak + ent_peak + legend
            st.altair_chart(chart_time, width="stretch")

    else:
        st.info("Selecione ao menos uma sprint para visualizar o comparativo por Tag.")

    # Resumo por Dev
    st.subheader("Resumo por Dev (seleção atual)")
    members_df = compute_member_metrics_for_cycles(
        cycle_ids=effective_cycle_ids,
        project_id=project_id,
        assignee_ids=assignee_ids or None,
        label_ids=label_ids or None,
        state_ids=state_ids or None,
    )
    if members_df.empty:
        st.info("Nenhum dado por Dev encontrado com os filtros atuais.")
    else:
        members_df["Lead Time médio (dias)"] = pd.to_numeric(members_df["Lead Time médio (dias)"], errors="coerce").round(1)
        members_df["Cycle Time médio (dias)"] = pd.to_numeric(members_df["Cycle Time médio (dias)"], errors="coerce").round(1)
        members_df["Pontos médios por issue"] = pd.to_numeric(members_df["Pontos médios por issue"], errors="coerce").round(1)
        st.dataframe(members_df, width='stretch')

    # Abas de tabelas
    st.subheader("Tabelas interativas")
    tab_sel, tab_cur, tab_back = st.tabs(["Selecionadas", "Sprint atual", "Backlog"])

    with tab_sel:
        fcol1, fcol2, fcol3, fcol4 = st.columns(4)
        if "worked_filter" not in st.session_state:
            st.session_state["worked_filter"] = "Todas"
        with fcol1:
            if st.button("Mostrar todas"):
                st.session_state["worked_filter"] = "Todas"
        with fcol2:
            if st.button("Ver não entregues"):
                st.session_state["worked_filter"] = "Não entregue"
        with fcol3:
            if st.button("Ver entregues"):
                st.session_state["worked_filter"] = "Entregue"
        with fcol4:
            if st.button("Ver com alertas"):
                st.session_state["worked_filter"] = "Com alerta"

        worked_df = load_issues_for_cycles(
            effective_cycle_ids,
            project_id,
            assignee_ids or None,
            label_ids or None,
            state_ids or None,
        )
        if not worked_df.empty:
            choice = st.session_state.get("worked_filter", "Todas")
            if choice == "Entregue":
                worked_df = worked_df[worked_df["Entrega"] == "Entregue"]
            elif choice == "Não entregue":
                worked_df = worked_df[worked_df["Entrega"] == "Não entregue"]
            elif choice == "Com alerta":
                worked_df = worked_df[worked_df["Alertas"].fillna("").str.strip() != ""]
            st.caption(f"Filtro: {choice}. Total de tarefas: {len(worked_df)}")
        if worked_df.empty:
            st.info("Nenhuma tarefa encontrada nas sprints selecionadas com os filtros atuais.")
        else:
            st.dataframe(worked_df, width='stretch')

    with tab_cur:
        ccol1, ccol2, ccol3, ccol4 = st.columns(4)
        if "current_filter" not in st.session_state:
            st.session_state["current_filter"] = "Todas"
        with ccol1:
            if st.button("Mostrar todas", key="cur_all"):
                st.session_state["current_filter"] = "Todas"
        with ccol2:
            if st.button("Ver não entregues", key="cur_not_delivered"):
                st.session_state["current_filter"] = "Não entregue"
        with ccol3:
            if st.button("Ver entregues", key="cur_delivered"):
                st.session_state["current_filter"] = "Entregue"
        with ccol4:
            if st.button("Ver com alertas", key="cur_alerts"):
                st.session_state["current_filter"] = "Com alerta"

        current_df = load_issues_for_current_sprint(
            project_id,
            assignee_ids or None,
            label_ids or None,
            state_ids or None,
        )
        if not current_df.empty:
            choice_cur = st.session_state.get("current_filter", "Todas")
            if choice_cur == "Entregue":
                current_df = current_df[current_df["Entrega"] == "Entregue"]
            elif choice_cur == "Não entregue":
                current_df = current_df[current_df["Entrega"] == "Não entregue"]
            elif choice_cur == "Com alerta":
                current_df = current_df[current_df["Alertas"].fillna("").str.strip() != ""]
            st.caption(f"Filtro: {choice_cur}. Total de tarefas: {len(current_df)}")
        if current_df.empty:
            st.info("Nenhuma tarefa encontrada na sprint atual com os filtros atuais.")
        else:
            st.dataframe(current_df, width='stretch')

    with tab_back:
        bcol1, bcol2 = st.columns(2)
        if "backlog_filter" not in st.session_state:
            st.session_state["backlog_filter"] = "Todas"
        with bcol1:
            if st.button("Mostrar todas", key="back_all"):
                st.session_state["backlog_filter"] = "Todas"
        with bcol2:
            if st.button("Ver com alertas", key="back_alerts"):
                st.session_state["backlog_filter"] = "Com alerta"

        backlog_df = load_backlog_issues(
            project_id,
            assignee_ids or None,
            label_ids or None,
            state_ids or None,
        )
        if backlog_df.empty:
            st.info("Nenhuma tarefa no backlog com os filtros atuais.")
        else:
            choice_back = st.session_state.get("backlog_filter", "Todas")
            if choice_back == "Com alerta":
                backlog_df = backlog_df[backlog_df["Alertas"].fillna("").str.strip() != ""]
            st.caption(f"Filtro: {choice_back}. Total de tarefas: {len(backlog_df)}")
            st.dataframe(backlog_df, width='stretch')


if __name__ == "__main__":
    main()

