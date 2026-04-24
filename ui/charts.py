"""Shared Plotly styling for a calm, consistent chart design."""

from __future__ import annotations

import plotly.graph_objects as go

CHART_COLORS = {
    "primary": "#3b82f6",
    "secondary": "#7fb0ff",
    "positive": "#22c55e",
    "warning": "#f59e0b",
    "negative": "#ef4444",
    "muted": "#93a1b8",
    "grid": "rgba(147, 161, 184, 0.16)",
    "bg": "rgba(8, 15, 29, 0)",
}


def apply_consistent_layout(fig: go.Figure, *, height: int, top_margin: int = 28, show_legend: bool = True) -> go.Figure:
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=CHART_COLORS["bg"],
        plot_bgcolor=CHART_COLORS["bg"],
        margin=dict(l=8, r=8, t=top_margin, b=8),
        height=height,
        showlegend=show_legend,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10, color=CHART_COLORS["muted"]),
            bgcolor="rgba(0,0,0,0)",
            borderwidth=0,
            itemclick="toggle",
            itemdoubleclick="toggleothers",
        ),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="#131d33",
            bordercolor="rgba(147,161,184,0.32)",
            font=dict(color="#e8edf7", size=11),
        ),
        xaxis=dict(
            gridcolor=CHART_COLORS["grid"],
            zeroline=False,
            tickfont=dict(size=10, color=CHART_COLORS["muted"]),
            title_font=dict(size=11, color=CHART_COLORS["muted"]),
            linecolor="rgba(147,161,184,0.24)",
        ),
        yaxis=dict(
            gridcolor=CHART_COLORS["grid"],
            zeroline=False,
            tickfont=dict(size=10, color=CHART_COLORS["muted"]),
            title_font=dict(size=11, color=CHART_COLORS["muted"]),
            linecolor="rgba(147,161,184,0.24)",
        ),
    )
    return fig
