# Power BI / Tableau Dashboard Specifications

## Target Visualizations
1. **Total Minutes & Discovery Trends over Time:** Line chart tracking listening volume by genre and persona.
2. **Genre Skip Rate Analysis:** Grouped bar chart comparing skip percentages across music genres.
3. **Spotify Wrapped User Year:** Card visuals & tables highlighting top artists, top tracks, total minutes played, and peak listening hours.

## Serving Model Architecture
- **Engine:** Power BI VertiPaq (Import mode).
- **Primary Fact Table:** `gold.wrapped_user_year` (OBT) and `gold.fact_listening`.
