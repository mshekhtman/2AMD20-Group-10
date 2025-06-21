@echo off
REM ───────────────────────────────────────────────────────────────────────
REM    RQ3 Full Pipeline for Windows
REM ───────────────────────────────────────────────────────────────────────

REM 1) Ensure output dirs exist
if not exist data\new_rq3\plots (
    mkdir data\new_rq3
    mkdir data\new_rq3\plots
)

REM 2) ETL → final_dataset.csv
echo [1/6] Running analysis_rq3.py ...
python scripts\RQ3\analysis_rq3.py

REM 3) EDA plots
echo [2/6] Running plot_datasets_summaries.py ...
python scripts\RQ3\plot_datasets_summaries.py

REM 4) Build RDF KG
echo [3/6] Running rq3_kg-builder.py ...
python scripts\RQ3\rq3_kg-builder.py

REM 5) SPARQL query → above_avg_airports.txt
echo [4/6] Running run_sparql_rq3.py ...
python scripts\RQ3\run_sparql_rq3.py > data\new_rq3\above_avg_airports.txt

REM 6) KG excerpt visualization
echo [5/6] Running visualize_kg.py ...
python scripts\RQ3\visualize_kg.py

REM 6) Correlation stats
echo [6/9] Running correlation_analysis.py ...
python scripts\RQ3\correlation_analysis.py

REM 7) Annotated scatter plots
echo [7/9] Running plot_with_annotations.py ...
python scripts\RQ3\plot_with_annotations.py

REM 8) SHACL validation
echo [8/9] Running shacl_validate.py ...
python scripts\RQ3\shacl_validate.py

echo.
echo [9/9] All analyses complete. Check data\new_rq3 for outputs.
pause