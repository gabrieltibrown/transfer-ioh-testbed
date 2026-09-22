# Getting data out of the Philips patient monitoring ecosystem

To the best of my knowledge, the following are the available routes for moving
waveform and/or numeric data from the Philips patient monitoring ecosystem into
an external system.

This list is assembled from publicly available Philips documentation and from
peer-reviewed descriptions of deployed systems. It should not be treated as
exhaustive. Most Philips integration documentation sits behind the InCenter
customer portal or under non-disclosure, no public specification of the Data
Warehouse Connect schema or its delivery cadence could be located, and the
publicly available PIIC iX technical data sheet describes Release B.01 [2],
which is several releases behind current. The definitive list for any given site
is the interface catalogue Philips maintains for the installed release.

## Continuous waveform data

**Data Warehouse Connect (DWC).** A licensed PIIC iX feature that exports
"patient data, including waves, alarms, events, and trends (both admitted and
discharged) ... directly from surveillance PIIC iX stations to long-term data
storage", which Philips describes as "designed for clinical research" [2].
Malunjkar et al., describing a 500-bed PIC iX deployment at Stanford Children's
Hospital, confirm DWC is "part of the PIC iX system", stores monitor, telemetry
and IntelliBridge device data "in an enterprise level SQL database", and
captures "continuous waveforms such as Electrocardiogram (ECG) and invasive
pressures" alongside 1 Hz numerics [3]. Because DWC writes continuously to SQL
rather than dumping at discharge, near-real-time access is achievable by tailing
the database rather than taking periodic extracts. This appears to be the
approach taken in the platform of Joachim et al., which places Mirth Connect, a
database-polling integration engine, between DWC and Kafka [4]. Philips
publishes no cadence or latency figure for DWC. Daily row counts reported by
Malunjkar et al. imply that waveform rows are multi-second blocks, on the order
of ten seconds, which would place a floor on achievable data freshness through
this route [3].

**Capsule Medical Device Information Platform (MDIP).** Philips' device
connectivity platform, positioned upstream of PIIC iX. Philips describes it as
liberating "live-streaming patient data from nearly any medical device" and
supplying "clinical data to over 100+ downstream systems" in addition to feeding
PIC iX [1]. It is the most plausible route to sub-second waveform delivery at
department scale, but no interface specification or latency figure is published,
and it is a separately licensed product.

**Monitor Data Export Interface (DEI) over MIB/RS232.** The IntelliVue monitor's
own export interface. Waveform observed values are delivered in 256 ms packets
(128 samples at 500 sps, 32 at 125 sps, 16 at 62.5 sps), with real-time numerics
at 1 s [5]. This is genuinely real-time, and serial communication "is always
possible (except with MP2/X2)" even where the monitor is networked [5]. The
limitation is physical rather than protocol: one cabled connection per monitor,
which does not scale to a department.

**Monitor Data Export Interface over LAN.** The same interface over Ethernet,
and unavailable in any central-monitoring deployment: "The Data Export Interface
of IntelliVue patient monitors cannot be accessed via the Local Area Network
when the monitor is connected to the Philips LAN, e.g. to an Information Center
(central station)" [5].

## Numeric data only

**HL7 outbound from PIIC iX.** PIIC iX sends IHE-compliant HL7 messages
containing patient monitor parameter data, external device data, alert data and
physiological calculations to the EMR over TCP/IP, supporting HL7 v2.3 and v2.4
for Classic and Vista profiles and v2.6 for the IHE profile. Philips states that
"All patient numeric data is exported via HL7" [2]; waveforms are not carried.

**IntelliBridge Enterprise (IBE).** Philips' interoperability engine, which
"provides HL7 interface interoperability between many Philips products" and
external systems [6]. Relevant here mainly as a routing and mapping layer over
the interfaces above rather than as an additional source of continuous data: its
Wave Form Export licence "permits connections to the IntelliVue Information
Center to capture waveform snippets and send using HL7" [6], which is episodic
rather than continuous.

## Episodic or non-time-series exports

**Wave Strip Export.** Alarm and saved strips exported as `.png` image files to
a configured server, showing up to 20 waves [2]. Images rather than data.

**Holter Export.** A licensed feature storing patient ADT and ECG wave data,
including 12-lead, in a repository for the Philips Holter monitoring system, for
a user-selected duration of up to 96 hours [2].

**12-lead ECG export.** Diagnostic 12-lead captures exported to a compatible
cardiology management system such as TraceMasterVue or GE Muse [2].

## Third-party capture platforms

Several commercial systems capture continuous waveforms from Philips monitors
for research and clinical surveillance, among them MediCollector, BedMaster EX,
Sickbay and ixTrend. Of these only MediCollector's specification was checked
directly: MediCollector CENTRAL captures waveform and numeric data from up to 50
networked medical devices, requires device-specific cabling, and can stream data
out over TCP, HL7 v2.6 or HL7 FHIR [7]. These products generally attach at the
monitor or network level rather than through PIIC iX, and none publish latency
figures.

## Summary

Two routes carry continuous waveform data at department scale: **Data Warehouse
Connect**, which is near-real-time at multi-second granularity, and **Capsule
MDIP**, which is designed for live streaming but undocumented publicly. The
monitor's own Data Export Interface is genuinely real-time but is either
unavailable on the clinical LAN or requires per-bed cabling. Everything else
carries numerics, images or episodic snippets.

## References

1. Philips. *PIC iX and Capsule MDIP integration.*
   https://www.usa.philips.com/healthcare/technology/picix-capsule-mdip-integration
2. Philips. *IntelliVue Information Center iX Release B.01 Technical Data Sheet.*
   https://www.biyomar.com/uploads/katalog/monitor-cihazlari/piic-ix.pdf
3. Malunjkar S, Weber S, Datta S. (2021). *A highly scalable repository of
   waveform and vital signs data from bedside monitoring devices.* arXiv:2106.03965.
   https://arxiv.org/abs/2106.03965
4. Joachim J, Chamoux T, Perdereau J, Iovène V, Moreau T, Cartailler J, Vallée F.
   (2026). *Real-time peri-operative data integration and clinical decision
   support: an open interoperable platform for patient data.* European Journal of
   Anaesthesiology. https://doi.org/10.1097/EJA.0000000000002422
5. Philips. *Data Export Interface Programming Guide. IntelliVue X2, MP Series &
   MX Series, Avalon FM Series.* Part 453564588011, 08/2015.
   https://www.documents.philips.com/doclib/enc/fetch/2000/4504/577242/577243/577247/582636/582882/X2%2C_MP%2C_MX_&_FM_Series_Rel._L.0_Data_Export_Interface_Program._Guide_4535_645_88011_(ENG).pdf
6. Philips. *IntelliBridge Enterprise Release B.18 Technical Data Sheet.*
   https://www.documents.philips.com/assets/20221212/bc88e09cf1bf4d3b8ea5af6900d099d7.pdf
7. MediCollector. *MediCollector CENTRAL.*
   https://www.medicollector.com/medicollector-central.html
