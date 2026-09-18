from spyion_client import spyion_extract, mf

# Optional: see how many cells match before pulling (reads only parquet footers)
print(spyion_extract(
    "cycling.cycle",
    filters=[
        mf("Associated Task ID", "357", conj="AND"),    # base term
        mf("Associated Task ID", "346", conj="OR"),
        mf("name",           "R0003",   conj="AND"),
        mf("Anode Material", "Anovion", conj="AND"),
        mf("Custom Name",    "um",      conj="AND NOT"), # the "NOT" chip
    ],
    dry_run=True,
))

# The actual pull
df = spyion_extract(
    "cycling.cycle",
    filters=[
        mf("Associated Task ID", "357", conj="AND"),
        mf("Associated Task ID", "346", conj="OR"),
        mf("name",           "R0003",   conj="AND"),
        mf("Anode Material", "Anovion", conj="AND"),
        mf("Custom Name",    "um",      conj="AND NOT"),
    ],
    columns=["Cycle_Number", "Discharge_Capacity"],   # underscores, not the UI labels
)

print(df.head())