from __future__ import annotations

from io import BytesIO
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from fastapi import HTTPException


def fetch_average_return_frame(conn) -> pd.DataFrame:
    query = """
        SELECT
            fight_date,
            AVG(model_return)::float AS average_return,
            COUNT(*) AS fight_count
        FROM public.historical_predictions
        GROUP BY fight_date
        ORDER BY fight_date
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [column.name for column in cur.description]

    if not rows:
        raise HTTPException(
            status_code=409,
            detail="No historical prediction returns are available.",
        )

    frame = pd.DataFrame(rows, columns=columns)
    frame["fight_date"] = pd.to_datetime(frame["fight_date"])
    frame["average_return"] = pd.to_numeric(frame["average_return"])
    frame["fight_count"] = pd.to_numeric(frame["fight_count"])
    return frame


def render_average_return_chart(conn: Any) -> bytes:
    frame = fetch_average_return_frame(conn)

    sns.set_theme(
        style="darkgrid",
        rc={
            "axes.facecolor": "#34393f",
            "figure.facecolor": "#34393f",
            "grid.color": "#4e545c",
            "text.color": "#e7e9ec",
            "axes.labelcolor": "#e7e9ec",
            "xtick.color": "#e3e5e5",
            "ytick.color": "#e3e5e5",
        },
    )

    fig, ax = plt.subplots(figsize=(11, 5.5), dpi=160)

    sns.lineplot(
        data=frame,
        x="fight_date",
        y="average_return",
        marker="o",
        linewidth=2.5,
        markersize=7,
        color="#f08a24",
        ax=ax,
    )

    ax.axhline(1.0, color="#e7e9ec", linestyle="--", linewidth=1.2, alpha=0.75)
    ax.set_title("Average Return by Fight Date", fontsize=18, fontweight="bold", pad=16)
    ax.set_xlabel("Fight Date", labelpad=10)
    ax.set_ylabel("Average Return", labelpad=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.tick_params(axis="x", rotation=35)
    ax.margins(x=0.04, y=0.12)

    for spine in ax.spines.values():
        spine.set_color("#4e545c")

    buffer = BytesIO()
    fig.tight_layout()
    fig.savefig(buffer, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()
