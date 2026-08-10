from __future__ import annotations

from io import BytesIO
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd
import seaborn as sns
from fastapi import HTTPException


def fetch_cumulative_profit_frame(conn) -> pd.DataFrame:
    query = """
        SELECT
            fight_date,
            SUM(
                CASE
                    WHEN model_pick = red_fighter THEN
                        CASE
                            WHEN model_return > 1 THEN 1.0
                            WHEN red_odds < 0 THEN -(ABS(red_odds)::float / 100.0)
                            ELSE -(100.0 / NULLIF(red_odds, 0))
                        END
                    WHEN model_pick = blue_fighter THEN
                        CASE
                            WHEN model_return > 1 THEN 1.0
                            WHEN blue_odds < 0 THEN -(ABS(blue_odds)::float / 100.0)
                            ELSE -(100.0 / NULLIF(blue_odds, 0))
                        END
                    ELSE 0.0
                END
            )::float AS event_units,
            COUNT(*) AS bet_count
        FROM public.historical_predictions
        WHERE model_pick != 'Pass'
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
            detail="No historical suggested bet returns are available.",
        )

    frame = pd.DataFrame(rows, columns=columns)
    frame["fight_date"] = pd.to_datetime(frame["fight_date"])
    frame["event_units"] = pd.to_numeric(frame["event_units"])
    frame["bet_count"] = pd.to_numeric(frame["bet_count"])
    frame["cumulative_units"] = frame["event_units"].cumsum()
    return frame


def fetch_top_betting_events_frame(conn) -> pd.DataFrame:
    query = """
        SELECT
            event_name,
            AVG(model_return)::float AS average_return,
            COUNT(*) AS fight_count,
            fight_date
        FROM public.historical_predictions
        WHERE model_return != 1
          AND event_name IS NOT NULL
        GROUP BY event_name, fight_date
        ORDER BY average_return DESC, fight_count DESC, fight_date DESC
        LIMIT 5
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [column.name for column in cur.description]

    if not rows:
        raise HTTPException(
            status_code=409,
            detail="No betting event returns are available.",
        )

    frame = pd.DataFrame(rows, columns=columns)
    frame["fight_date"] = pd.to_datetime(frame["fight_date"])
    frame["average_return"] = pd.to_numeric(frame["average_return"])
    frame["fight_count"] = pd.to_numeric(frame["fight_count"])
    return frame


def fetch_suggested_bet_confidence_frame(conn: Any) -> pd.DataFrame:
    query = """
        SELECT
            red_fighter,
            blue_fighter,
            red_odds,
            blue_odds,
            model_picked_red_winner,
            confidence,
            model_pick,
            model_return
        FROM public.historical_predictions
        WHERE model_pick != 'Pass'
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [column.name for column in cur.description]

    if not rows:
        raise HTTPException(
            status_code=409,
            detail="No historical suggested bets are available.",
        )

    frame = pd.DataFrame(rows, columns=columns)
    frame["red_odds"] = pd.to_numeric(frame["red_odds"], errors="coerce")
    frame["blue_odds"] = pd.to_numeric(frame["blue_odds"], errors="coerce")
    frame["confidence"] = pd.to_numeric(frame["confidence"], errors="coerce")
    frame["model_return"] = pd.to_numeric(frame["model_return"], errors="coerce")
    if not pd.api.types.is_bool_dtype(frame["model_picked_red_winner"]):
        frame["model_picked_red_winner"] = frame["model_picked_red_winner"].astype(str).str.lower().eq("true")
    return frame


def set_chart_theme() -> None:
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


def save_figure_to_png(fig) -> bytes:
    buffer = BytesIO()
    fig.tight_layout()
    fig.savefig(buffer, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def render_average_return_chart(conn: Any) -> bytes:
    frame = fetch_cumulative_profit_frame(conn)

    set_chart_theme()

    fig, ax = plt.subplots(figsize=(11, 5.5), dpi=160)

    sns.lineplot(
        data=frame,
        x="fight_date",
        y="cumulative_units",
        marker="o",
        linewidth=2.5,
        markersize=7,
        color="#f08a24",
        ax=ax,
    )

    ax.axhline(0.0, color="#e7e9ec", linestyle="--", linewidth=1.2, alpha=0.75)
    ax.set_title("Odds-Weighted Cumulative Model Profit by Fight Date", fontsize=18, fontweight="bold", pad=16)
    ax.set_xlabel("Fight Date", labelpad=10)
    ax.set_ylabel("Units Won/Lost", labelpad=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.tick_params(axis="x", rotation=35)
    ax.margins(x=0.04, y=0.12)

    for spine in ax.spines.values():
        spine.set_color("#4e545c")

    return save_figure_to_png(fig)


def render_suggested_bet_confidence_chart(conn: Any) -> bytes:
    frame = fetch_suggested_bet_confidence_frame(conn)

    red_bet_mask = frame["model_pick"].eq(frame["red_fighter"])
    blue_bet_mask = frame["model_pick"].eq(frame["blue_fighter"])
    suggested_bet_mask = red_bet_mask | blue_bet_mask

    frame = frame.loc[suggested_bet_mask].copy()
    if frame.empty:
        raise HTTPException(status_code=409, detail="No historical suggested bets are available.")

    frame["suggested_bet_odds"] = frame["red_odds"].where(red_bet_mask.loc[frame.index], frame["blue_odds"])
    picked_suggested_side = frame["model_picked_red_winner"].where(
        red_bet_mask.loc[frame.index],
        ~frame["model_picked_red_winner"],
    )
    frame["suggested_bet_confidence"] = frame["confidence"].where(picked_suggested_side, 1 - frame["confidence"])
    frame["bet_result"] = frame["model_return"].gt(1).map({True: "Correct Bet", False: "Incorrect Bet"})
    frame = frame.dropna(subset=["suggested_bet_odds", "suggested_bet_confidence", "bet_result"])
    frame = frame.sort_values("suggested_bet_odds")

    set_chart_theme()
    grid = sns.relplot(
        data=frame,
        x="suggested_bet_odds",
        y="suggested_bet_confidence",
        hue="bet_result",
        hue_order=["Correct Bet", "Incorrect Bet"],
        palette={"Correct Bet": "#5fd38d", "Incorrect Bet": "#f05f5f"},
        kind="scatter",
        height=5.5,
        aspect=2,
        s=90,
        edgecolor="#222529",
        linewidth=0.8,
    )

    fig = grid.figure
    fig.set_dpi(160)
    fig.patch.set_facecolor("#34393f")
    ax = grid.ax
    ax.set_facecolor("#34393f")
    ax.axvline(0, color="#e7e9ec", linestyle="--", linewidth=1.2, alpha=0.75)
    ax.axhline(0.5, color="#e7e9ec", linestyle=":", linewidth=1.4, alpha=0.85)
    ax.set_title("Suggested Bet Confidence by Odds with Results", fontsize=18, fontweight="bold", pad=16)
    ax.set_xlabel("<<--Favorites--  |  Suggested Bet Odds  |  >>--Underdogs-->>", labelpad=10)
    ax.set_ylabel("Confidence in Suggested Bet", labelpad=10)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_ylim(0, 1)
    ax.margins(x=0.08, y=0.12)

    for spine in ax.spines.values():
        spine.set_color("#4e545c")

    if grid.legend:
        grid.legend.set_title("Bet Result")
        grid.legend.get_frame().set_facecolor("#34393f")
        grid.legend.get_frame().set_edgecolor("#4e545c")

    return save_figure_to_png(fig)


def render_top_betting_events_chart(conn: Any) -> bytes:
    frame = fetch_top_betting_events_frame(conn).copy()
    frame["average_return"] = frame["average_return"].map(lambda value: f"{value:.2f}")
    frame["fight_count"] = frame["fight_count"].astype(int).astype(str)
    frame["fight_date"] = frame["fight_date"].dt.strftime("%b %d, %Y")
    frame = frame.rename(
        columns={
            "event_name": "Event Name",
            "average_return": "Average Return",
            "fight_count": "Number of Fights",
            "fight_date": "Date of Event",
        }
    )

    sns.set_theme(style="dark")
    fig, ax = plt.subplots(figsize=(11, 3.8), dpi=160)
    fig.patch.set_facecolor("#34393f")
    ax.set_facecolor("#34393f")
    ax.axis("off")
    ax.set_title("Top 5 Betting Events", fontsize=18, fontweight="bold", color="#e7e9ec", pad=16)

    table = ax.table(
        cellText=frame[["Event Name", "Average Return", "Number of Fights", "Date of Event"]].values,
        colLabels=["Event Name", "Average Return", "Number of Fights", "Date of Event"],
        cellLoc="left",
        colLoc="left",
        loc="center",
        colWidths=[0.48, 0.17, 0.17, 0.18],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.65)

    for (row_idx, _), cell in table.get_celld().items():
        cell.set_edgecolor("#4e545c")
        cell.set_linewidth(0.8)
        if row_idx == 0:
            cell.set_facecolor("#222529")
            cell.set_text_props(color="#f08a24", weight="bold")
        else:
            cell.set_facecolor("#34393f" if row_idx % 2 else "#3b4047")
            cell.set_text_props(color="#e7e9ec")

    return save_figure_to_png(fig)
