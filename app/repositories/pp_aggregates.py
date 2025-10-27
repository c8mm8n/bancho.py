from __future__ import annotations

import math
from typing import cast

import app.state.services


async def update_player_pp_aggregates(player_id: int) -> None:
    """Update PP aggregates for a player.
    
    Fetches all stats for the player (modes 0-3, 4-6, 8),
    calculates total PP and standard deviation for all_modes, classic, and relax,
    and updates the player_pp_aggregates table.
    """
    # Fetch all stats for the player
    stats_rows = await app.state.services.database.fetch_all(
        "SELECT mode, pp FROM stats WHERE id = :player_id AND mode IN (0, 1, 2, 3, 4, 5, 6, 8)",
        {"player_id": player_id},
    )
    
    if not stats_rows:
        return
    
    # Group stats by category
    classic_modes = [0, 1, 2, 3]  # vanilla/classic modes
    relax_modes = [4, 5, 6, 8]     # relax modes
    
    classic_pp_values = []
    relax_pp_values = []
    all_pp_values = []
    
    for row in stats_rows:
        mode = row["mode"]
        pp = row["pp"]
        
        all_pp_values.append(pp)
        
        if mode in classic_modes:
            classic_pp_values.append(pp)
        elif mode in relax_modes:
            relax_pp_values.append(pp)
    
    # Calculate totals and stddev for each category
    def calculate_stats(values: list[float]) -> tuple[float, float]:
        if not values:
            return (0.0, 0.0)
        
        total = sum(values)
        
        # Calculate standard deviation
        if len(values) <= 1:
            stddev = 0.0
        else:
            mean = total / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            stddev = math.sqrt(variance)
        
        return (total, stddev)
    
    all_total, all_stddev = calculate_stats(all_pp_values)
    classic_total, classic_stddev = calculate_stats(classic_pp_values)
    relax_total, relax_stddev = calculate_stats(relax_pp_values)
    
    # UPSERT into player_pp_aggregates using REPLACE INTO
    await app.state.services.database.execute(
        """
        REPLACE INTO player_pp_aggregates (
            player_id,
            all_modes_total, all_modes_stddev,
            classic_total, classic_stddev,
            relax_total, relax_stddev
        ) VALUES (
            :player_id,
            :all_modes_total, :all_modes_stddev,
            :classic_total, :classic_stddev,
            :relax_total, :relax_stddev
        )
        """,
        {
            "player_id": player_id,
            "all_modes_total": all_total,
            "all_modes_stddev": all_stddev,
            "classic_total": classic_total,
            "classic_stddev": classic_stddev,
            "relax_total": relax_total,
            "relax_stddev": relax_stddev,
        },
    )
