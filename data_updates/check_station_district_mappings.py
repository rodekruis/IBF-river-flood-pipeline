from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch


def parse_args() -> argparse.Namespace:
	repo_root = Path(__file__).resolve().parents[1]
	default_git_lfs_root = (
		repo_root.parent / "IBF-system" / "services" / "API-service" / "src" / "scripts" / "git-lfs"
	)
	parser = argparse.ArgumentParser(
		description="Plot one map per GloFAS station with its associated administrative divisions."
	)
	parser.add_argument(
		"--mapping-dir",
		type=Path,
		default=repo_root / "config",
		help="Directory containing *_station_district_mapping.csv files.",
	)
	parser.add_argument(
		"--git-lfs-root",
		type=Path,
		default=default_git_lfs_root,
		help="Path to IBF-system git-lfs root containing admin-boundaries/ and point-layers/.",
	)
	parser.add_argument(
		"--output-dir",
		type=Path,
		default=repo_root / "data_updates" / "output" / "station_admin_maps",
		help="Directory where PNG maps will be written.",
	)
	parser.add_argument(
		"--countries",
		nargs="+",
		default=None,
		help="Optional list of ISO3 country codes to process.",
	)
	return parser.parse_args()


def find_pcode_column(columns: list[str], admin_level: int | None = None) -> str | None:
	# If we know the admin level, look for the exact ADMx_PCODE column first
	if admin_level is not None:
		target = f"ADM{admin_level}_PCODE"
		match = [c for c in columns if c.upper() == target]
		if match:
			return match[0]
	# Fallback: plain PCODE
	exact = [column for column in columns if column.upper() == "PCODE"]
	if exact:
		return exact[0]
	# Fallback: highest-level ADMx_PCODE present
	candidates = [column for column in columns if "PCODE" in column.upper()]
	if not candidates:
		return None
	candidates.sort(key=lambda value: value.upper(), reverse=True)
	return candidates[0]


def load_mapping(mapping_file: Path) -> pd.DataFrame:
	mapping = pd.read_csv(mapping_file, dtype={"placeCode": str, "glofasStation": str})
	if "glofasStation" not in mapping.columns or "placeCode" not in mapping.columns:
		raise ValueError(
			f"Expected columns 'glofasStation' and 'placeCode' in {mapping_file.name}, "
			f"found: {mapping.columns.tolist()}"
		)
	mapping = mapping[["glofasStation", "placeCode"]].copy()
	mapping["glofasStation"] = mapping["glofasStation"].astype(str).str.strip()
	mapping["placeCode"] = mapping["placeCode"].astype(str).str.strip()
	mapping = mapping[(mapping["glofasStation"] != "") & (mapping["placeCode"] != "")]
	return mapping


def load_stations(country: str, point_layers_dir: Path) -> gpd.GeoDataFrame:
	station_file = point_layers_dir / f"glofas_stations_{country}.csv"
	if not station_file.exists():
		raise FileNotFoundError(f"Station file not found: {station_file}")

	stations = pd.read_csv(station_file)
	required_columns = {"stationCode", "lat", "lon"}
	missing_columns = sorted(required_columns.difference(stations.columns))
	if missing_columns:
		raise ValueError(f"Missing columns {missing_columns} in {station_file}")

	stations = stations.copy()
	stations["stationCode"] = stations["stationCode"].astype(str).str.strip()
	stations["lat"] = pd.to_numeric(stations["lat"], errors="coerce")
	stations["lon"] = pd.to_numeric(stations["lon"], errors="coerce")
	stations = stations.dropna(subset=["lat", "lon"])

	return gpd.GeoDataFrame(
		stations,
		geometry=gpd.points_from_xy(stations["lon"], stations["lat"]),
		crs="EPSG:4326",
	)


def load_admin_boundaries(country: str, admin_boundaries_dir: Path) -> list[tuple[str, gpd.GeoDataFrame, str]]:
	boundary_files = sorted(admin_boundaries_dir.glob(f"{country}_adm*.json"))
	if not boundary_files:
		raise FileNotFoundError(f"No admin boundary files found for {country} in {admin_boundaries_dir}")

	boundaries: list[tuple[str, gpd.GeoDataFrame, str]] = []
	for boundary_file in boundary_files:
		gdf = gpd.read_file(boundary_file)
		# Extract admin level number from filename (e.g. ZMB_adm2 -> 2)
		level_str = boundary_file.stem.split("_")[-1].replace("adm", "")
		admin_level = int(level_str) if level_str.isdigit() else None
		pcode_column = find_pcode_column(list(gdf.columns), admin_level=admin_level)
		if pcode_column is None:
			continue
		gdf = gdf[[pcode_column, "geometry"]].copy()
		gdf[pcode_column] = gdf[pcode_column].astype(str).str.strip()
		level_label = boundary_file.stem.split("_")[-1]
		boundaries.append((level_label, gdf, pcode_column))

	if not boundaries:
		raise ValueError(f"No PCODE columns found in admin boundary files for {country}")
	return boundaries


def plot_station_map(
	country: str,
	station_code: str,
	station_geometry,
	matched_admin: gpd.GeoDataFrame,
	all_admin: gpd.GeoDataFrame,
	output_file: Path,
) -> None:
	fig, ax = plt.subplots(figsize=(8, 8))

	# Draw full country outline as context
	all_admin.plot(ax=ax, facecolor="#f0f0f0", edgecolor="lightgray", linewidth=0.4, zorder=1)

	# Highlight matched admin divisions
	if not matched_admin.empty:
		matched_admin.plot(ax=ax, facecolor="lightskyblue", edgecolor="steelblue", linewidth=0.8, zorder=2)
		# Label each matched polygon with its pcode
		for _, row in matched_admin.iterrows():
			centroid = row.geometry.centroid
			label = row.get("pcode", "")
			if label:
				ax.annotate(label, xy=(centroid.x, centroid.y), fontsize=5,
						ha="center", va="center", color="steelblue", zorder=4)

	# Plot station point
	station_gdf = gpd.GeoDataFrame(
		[{"stationCode": station_code}],
		geometry=[station_geometry],
		crs="EPSG:4326",
	)
	station_gdf.plot(ax=ax, color="red", markersize=60, zorder=5, edgecolor="darkred", linewidth=0.5)

	# Zoom to full country extent
	country_min_x, country_min_y, country_max_x, country_max_y = all_admin.total_bounds
	x_pad = (country_max_x - country_min_x) * 0.05
	y_pad = (country_max_y - country_min_y) * 0.05
	ax.set_xlim(country_min_x - x_pad, country_max_x + x_pad)
	ax.set_ylim(country_min_y - y_pad, country_max_y + y_pad)

	ax.set_title(f"{country} | Station {station_code}", fontsize=11)
	ax.set_axis_off()
	ax.legend(
		handles=[
			Patch(facecolor="lightskyblue", edgecolor="steelblue", label="Associated admin divisions"),
			Line2D([0], [0], marker="o", color="w", markerfacecolor="red", markersize=8, label="GloFAS station"),
		],
		loc="lower left",
	)

	output_file.parent.mkdir(parents=True, exist_ok=True)
	fig.savefig(output_file, dpi=200, bbox_inches="tight")
	plt.close(fig)


def main() -> None:
	args = parse_args()

	mapping_dir = args.mapping_dir
	point_layers_dir = args.git_lfs_root / "point-layers"
	admin_boundaries_dir = args.git_lfs_root / "admin-boundaries"
	output_dir = args.output_dir

	mapping_files = sorted(mapping_dir.glob("*_station_district_mapping.csv"))
	if not mapping_files:
		raise FileNotFoundError(f"No mapping files found in {mapping_dir}")

	requested_countries = {country.strip().upper() for country in args.countries} if args.countries else None

	for mapping_file in mapping_files:
		country = mapping_file.stem.split("_")[0].upper()
		if requested_countries and country not in requested_countries:
			continue

		print(f"Processing {country}...")
		mapping = load_mapping(mapping_file)
		stations = load_stations(country, point_layers_dir)
		boundaries = load_admin_boundaries(country, admin_boundaries_dir)

		all_admin = gpd.GeoDataFrame(
			pd.concat([boundary_gdf[["geometry"]] for _, boundary_gdf, _ in boundaries], ignore_index=True),
			geometry="geometry",
			crs="EPSG:4326",
		)

		station_to_pcodes = mapping.groupby("glofasStation")["placeCode"].apply(lambda values: sorted(set(values)))

		for station_code, pcodes in station_to_pcodes.items():
			station_row = stations[stations["stationCode"] == station_code]
			if station_row.empty:
				print(f"  - Skipping {station_code}: station not found in point-layers file")
				continue

			matched_parts: list[gpd.GeoDataFrame] = []
			# Build expanded pcode set: original + zero-padded variants to handle
			# leading-zero mismatches between mapping CSV and boundary files
			pcodes_expanded = set(pcodes)
			for pc in pcodes:
				pcodes_expanded.add(pc.zfill(len(pc) + 1))  # add leading zero
				pcodes_expanded.add(pc.lstrip("0") or pc)   # strip leading zeros
			for level_label, boundary_gdf, pcode_column in boundaries:
				matched = boundary_gdf[boundary_gdf[pcode_column].isin(pcodes_expanded)].copy()
				if matched.empty:
					continue
				matched = matched.rename(columns={pcode_column: "pcode"})
				matched["adminLevel"] = level_label
				matched_parts.append(matched)

			matched_admin = (
				gpd.GeoDataFrame(
					pd.concat(matched_parts, ignore_index=True),
					geometry="geometry",
					crs="EPSG:4326",
				)
				if matched_parts
				else gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")
			)

			output_file = output_dir / country / f"{station_code}.png"
			plot_station_map(
				country=country,
				station_code=station_code,
				station_geometry=station_row.iloc[0].geometry,
				matched_admin=matched_admin,
				all_admin=all_admin,
				output_file=output_file,
			)
			print(f"  - Wrote {output_file}")


if __name__ == "__main__":
	main()
