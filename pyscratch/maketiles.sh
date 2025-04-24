# Coarse hexes (low zoom)
#tippecanoe -o tiles/hex3.mbtiles -l hex3 -z4 --drop-densest-as-needed --no-tile-size-limit h3_hexagons_03.geojson --force

# Medium hexes (mid zoom)
#tippecanoe -o tiles/hex4.mbtiles -l hex4 -z7 --drop-densest-as-needed --no-tile-size-limit h3_hexagons_04.geojson --force

# Fine hexes (high zoom)
#tippecanoe   -o tiles/hex5.mbtiles   -l hex5   -z14   -Z8   --drop-smallest-as-needed   --no-line-simplification   --coalesce-densest-as-needed --extend-zooms-if-still-dropping  --force  h3_hexagons_05.geojson

#tippecanoe -o tiles/hex03.mbtiles -Z3 -z5 app/geo/h3_hexagons_03_closed.geojson --force --drop-rate=0 --no-feature-limit --no-tile-size-limit
#tippecanoe -o tiles/hex04.mbtiles -Z6 -z8 app/geo/h3_hexagons_04_closed.geojson --force --drop-rate=0 --no-feature-limit --no-tile-size-limit
#tippecanoe -o tiles/hex05.mbtiles -Z9 -z11 app/geo/h3_hexagons_05_closed.geojson --force --no-feature-limit

tippecanoe \
  -o tiles/hex_all.mbtiles \
  -L layer03:app/geo/h3_hexagons_03_zfiltered.geojson \
  -L layer04:app/geo/h3_hexagons_04_zfiltered.geojson \
  -L layer05:app/geo/h3_hexagons_05_zfiltered.geojson \
  -Z3 -z12 \
  --force \
  --drop-rate=0 \
  --no-feature-limit \
  --no-tile-size-limit


#tippecanoe -o tiles/hex5.mbtiles -l hex5 -z8 --drop-densest-as-needed --no-tile-size-limit h3_hexagons_05.geojson --force

#tippecanoe -o tiles/hex4.mbtiles -l hex5 -z5 --drop-densest-as-needed --no-tile-size-limit h3_hexagons_04.geojson --force

#tippecanoe -o tiles/hex3.mbtiles -l hex5 -z3 --drop-densest-as-needed --no-tile-size-limit h3_hexagons_03.geojson --force

#tippecanoe -o tiles/hex5points.mbtiles -Z8 app/geo/h3_points_05.geojson --force --drop-rate=0 --no-feature-limit --no-tile-size-limit
