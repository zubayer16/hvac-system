# Phase 1 thermal model

One effective thermal zone per building. During each interval the inputs are held constant:

`C dT/dt = H (Tout - T) + Qheat - Qcool + Qinternal`

C is effective thermal capacity (kWh/K); H = envelope conductance + 1.2 * airflow
(kW/K). The latter approximates air density times specific heat, with airflow in m3/s.
Internal gains are 0.1 kW/person plus 2 kW background. Occupancy is seeded and follows
weekday 08:00-18:00 at the configured fixed local offset. Weather has annual/daily cycles
and small seeded noise. All time serialized to events is UTC.

For constant inputs, `Teq = Tout + (Qheat - Qcool + Qinternal)/H`, and
`Tnext = Teq + (T - Teq) exp(-H dt/C)` with dt in hours. This exact interval solution avoids
Euler instability. The baseline controller requests H*(setpoint-Tout)-gains+12*(setpoint-T)
kW and saturates at equipment capacity. Positive/negative requests heat/cool respectively.
Estimated electrical energy is `((Qheat+Qcool)/COP + 5*(fan_fraction)^3)*dt` kWh.

| Building | C kWh/K | Envelope kW/K | HVAC kW | People | Max airflow m3/s | Setpoint C |
|---|---:|---:|---:|---:|---:|---:|
| office | 120 | 3 | 150 | 120 | 3 | 21 |
| retail | 200 | 5 | 220 | 200 | 5 | 22 |
| school | 160 | 4 | 180 | 180 | 4 | 21 |

COP is 3 and UTC offset is +1 for all archetypes. These are illustrative parameters,
not fitted measurements. No solar/latent loads, multi-zone exchange, thermal mass layers,
heat recovery, equipment cycling or calibrated moisture/CO2 dynamics are represented.
Supply temperature is a diagnostic proxy, not a conserved air-side energy balance.
Indoor/return temperature describe interval start; energy covers the following interval.
An injected heating fault reduces delivered heat to 45% while input energy stays unchanged.
A missing sensor changes only the observed indoor value, not the underlying temperature.
Fault labels are simulator ground truth and must never enter model feature columns.

Published operating ranges in the domain are broad quality rejection bounds, not comfort
or equipment safety limits. Null is allowed only for indoor temperature in this phase;
other missing sensor channels, configurable import units, late/reordered ingestion and
watermark handling will be added at ingestion boundaries in later phases.
