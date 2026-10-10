# Radio Planning for Concord Fleets

Concord assumes the network fails routinely and converges anyway. This
document is about making it fail rarely: how far stock radios reach,
which antenna belongs where, and how to lay out cells so density, not
raw node count, stays inside the protocol's comfort zone. Link numbers
are 2.4 GHz; shift by about -7 dB for 5 GHz planning, and read band
selection below before spending anything.

Companion documents: `docs/scaling.md` (protocol ceilings),
`docs/deployment.md` (Plan density), `docs/architecture.md` (anchors).

Band selection first, because it shapes everything below. No single
band wins outdoors; range and capacity pull opposite ways:

```
2.4 GHz   longest reach, best obstacle punch-through. Only 3
          non-overlapping channels, crowded spectrum. The default for
          single-cell coverage: one pit, one yard, one warehouse floor.

5 GHz     ~6-8 dB shorter through obstacles, but many more channels
          and far less congestion. The default for multi-cell sites:
          channels are what cell splitting spends, and 5 GHz has them.
          Fine outdoors over hundreds of meters with line of sight.

6 GHz     shortest reach, huge clean spectrum. Depot-yard dense cells
          and short high-rate links. Useless at pit scale.

sub-GHz   kilometers at milliwatts, kilobits per second. Position
          pings and telemetry only; cannot carry journal sync or images.

licensed (CBRS, private LTE/5G)   the real answer at mine scale:
          planned cells, real mobility, megabits. Needs a license or
          a carrier, which is an ops decision, not a radio tweak.
```

Rule: range-first means 2.4, capacity-first means 5, density-first
means 6, telemetry-only means sub-GHz, mine-scale means licensed. All
link numbers below are 2.4 GHz; shift by ~-7 dB for 5 GHz planning.

## 1. The one equation

Free-space path loss in dB, distance in km, frequency in MHz:

```
loss = 20*log10(d) + 20*log10(f) + 32.44
```

At 2400 MHz the last two terms total ~100, so the rule is brutal and
simple: **every doubling of distance costs 6 dB, forever**. Received
power is then:

```
RX = TX + TX_gain + RX_gain - loss - margin
```

Worked at stock power (20 dBm radio, 3 dBi whips both ends, 12 dB pit
margin), against a −75 dBm target (comfortable low rates, far more
than Concord's KB-scale traffic needs):

```
100 m:  20 + 3 + 3 - 80  - 12 = -66 dBm   fast
300 m:  20 + 3 + 3 - 90  - 12 = -76 dBm   full rate
500 m:  20 + 3 + 3 - 94  - 12 = -80 dBm   robust, plan here and below
1 km:   20 + 3 + 3 - 100 - 12 = -86 dBm   edge, needs gain or luck
```

Takeaway: stock radios cover a few hundred meters of pit with margin.
Anything farther is an antenna problem, never a power problem.

## 2. Power is the weakest lever

Range grows with the fourth root of power spending: +3 dB (double the
watts) buys 1.4x distance; doubling distance costs 6 dB (4x power).
Regulators cap effective radiated power before blasting gets useful
(FCC point-to-multipoint EIRP is about 4 W total, radio plus antenna
gain combined). So the design order is always: placement first, gain
second, rate third, power last. The client side stays near 100 mW in
every design below; range comes from the fixed side.

## 3. Antenna catalog: which, where, what gain

```
omni (stock whip, 2-3 dBi)
  doughnut pattern, hears all directions weakly
  USE: every vehicle, every robot, no exceptions

sector (panel, 12-16 dBi, 60-120 deg beam)
  wedge pattern, one strong slice of the compass
  USE: rim mast covering a pit, depot yard, one wall of a warehouse
  Three 120-deg sectors = full circle at ~14 dBi each

directional panel (18-24 dBi, narrow beam)
  flashlight pattern, one strong line
  USE: fixed point-to-point (mast to depot office), never for roamers

dish/grid (24-30 dBi)
  laser pattern, backhaul only
  USE: site to HQ, mast to fiber head. Useless for fleet access:
  a truck drives out of the beam in seconds
```

Gain is direction, not magic: a 14 dBi sector hears 25x better inside
its wedge and nearly nothing behind it. That selectivity is the point:
it rejects interference from outside the cell while reaching far
inside it.

Vehicle rule: roamers always carry omnis. A directional antenna on a
moving truck points at the sky, the ground, and other trucks in
rotation. Gain belongs on masts, which hold still.

## 4. Capacity per square kilometer

The binding constraint per cell is airtime contention, not signal.
CSMA degrades past roughly 20-30 active contenders regardless of bars:
radios spend their time deferring instead of sending. Concord's
appetite per node is small, about 50-100 kbps average, and a calm fleet
goes nearly quiet because gossip publishes only on change. But the
contender count is what it is, so plan cells by headcount:

```
nodes mutually in range     verdict
<= 20-30                    one cell, stock AP, no thought needed
30-60                       one cell works calm, split if all roam at once
60+                         split into sectors or channels, ~25 per cell
```

Worked example, 40-truck pit in 1 km^2. One 120-deg sector cell of
~600 m radius from the rim mast covers every truck at −70 dBm or
better. Forty contenders is over the comfort line, so split the pit
into two 60-deg sectors on different channels: ~20 trucks each, each
with full airtime, one anchor with backhaul behind both. Cost: one
extra panel antenna. The alternative, one big omni cell at higher
power, would be louder, slower, and illegal sooner.

Worked example, 200-node site. Eight cells of ~25 nodes on staggered
channels (1, 6, 11, then 5 GHz where clients support it), anchors as
the backhaul point per cell or per pair of cells. This is the same
"let distance segment for free" doctrine as the protocol layer:
partitions converge, so cells may overlap loosely without harm.

Counter-intuitive lever: **turn transmit power DOWN to shrink an
overfull cell**. A quieter AP holds fewer trucks, shedding the fringe
to the neighboring cell. Coverage holes are fixed by placement, never
by volume.

## 5. Partitioning for correct density: the procedure

```
1. MAP     Walk the site. Mark where machines dwell (loading bays,
           crusher, depot) vs transit (haul roads). Dwell points are
           cells; roads are overlap.

2. COUNT   Worst-case simultaneous contenders per area, not registered
           fleet size. Parked trucks with sleeping radios barely count;
           forty trucks mid-shift-change all gossip at once.

3. PLACE   One cell per ~25 contenders. Height beats power: a mast at
           15 m sees over dust and bodies that no wattage penetrates.
           Aim sectors down into the work area, not across it.

4. SPLIT   Different channels for adjacent cells (1/6/11 at 2.4 GHz;
           move multi-cell sites to 5 GHz where clients support it,
           channels are plentiful there). Same channel adjacent means
           one big contention domain no matter how many APs you bought.

5. ANCHOR  One rendezvous anchor per backhaul point, stable address,
           stable position. Anchors live where fiber and power live:
           depot office, rim mast, site server container.

6. MARGIN  Verify at −75 dBm or better everywhere machines work, with
           10-15 dB fade margin for dust, multipath off pit walls, and
           truck bodies. If a spot reads −80, move the antenna, not the
           slider.
```

Warehouse variant: racks are metal canyons. Treat each aisle zone as
its own cell with the AP at the aisle end firing down its length;
aisles act as waveguides and isolate nicely. One AP per 2-4 aisles is
typical; 5 GHz helps here because its shorter reach becomes free
isolation between zones.

Field variant (surveyor far out): no cell reaches it, and that is
fine. Options in order: an LTE cell within ~5 km, a satellite-fed
relay it visits, or plain delay tolerance. Partition, work, sync on
return. The protocol already handles all three; the radio plan only
has to stop pretending WLAN does 50 km.

## 6. How the protocol meets the radio

* Gossip is change-triggered, so a calm fleet is a quiet fleet. Busy
  airtime correlates with busy machines, exactly when fresh data
  matters.
* The 5s pull bursts are the heaviest periodic traffic: N-1 short
  sessions per node per tick. Size cells so a tick's burst fits the
  airtime with margin; the handshake budget table is in
  `docs/scaling.md`.
* mDNS speaks only at first contact. No steady-state multicast storm
  exists to plan around, unlike protocols with periodic multicast
  discovery.
* Journal deltas are small when calm (a few events per tick fleet-wide)
  and images/models move over the wired backhaul between anchors, never
  over the field WLAN if operations stages them at the depot first.

## 7. Checklist before calling a site done

* Every work area reads −75 dBm or better with fade margin.
* No cell holds more than ~25 simultaneous contenders at peak.
* Adjacent cells are on non-overlapping channels.
* Each cell or cell pair has an anchor with backhaul (fiber, LTE, sat).
* One truck driving the full site plan stays associated (roams, not
  drops) or partitions cleanly and resyncs on return, both verified,
  not assumed.
* The 200-node plan is more cells, never more watts.
