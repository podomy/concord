// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package geo holds geographic coordinates and distance math for anchor
// selection. It is deliberately tiny: anchors need a position, a validity
// check, and one distance function. Projections and formats belong in a
// mapping library, not here.
package geo

import "math"

// earthRadiusKM is the mean Earth radius used by the haversine.
const earthRadiusKM = 6371.0

// Point is a geographic position in decimal degrees.
type Point struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// Valid reports whether the point lies on the planet: latitude in
// [-90, 90], longitude in [-180, 180].
func (p Point) Valid() bool {
	return p.Lat >= -90 && p.Lat <= 90 && p.Lon >= -180 && p.Lon <= 180
}

// DistanceKM returns the great-circle distance between two points. Flat
// math on degrees breaks with scale (a degree of longitude is 111km at
// the equator and zero at the poles); the haversine costs the same ten
// lines and works from one pit to the whole planet.
func DistanceKM(a, b Point) float64 {
	lat1 := a.Lat * math.Pi / 180
	lat2 := b.Lat * math.Pi / 180
	dLat := (b.Lat - a.Lat) * math.Pi / 180
	dLon := (b.Lon - a.Lon) * math.Pi / 180

	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKM * math.Asin(math.Sqrt(h))
}
