"""
KLM Hub Analysis - Run All Visualizations

Master script to run all individual visualization scripts for the poster.
Runs each script separately to avoid memory issues and hanging.
Updated for correct project structure.
"""

import subprocess
import sys
import os
import time

def run_script(script_name, rdf_file):
    """Run a single visualization script"""
    print(f"\n{'='*60}")
    print(f"🎯 Running {script_name}...")
    print(f"{'='*60}")
    
    # Construct full path to script
    script_path = os.path.join("scripts", "Visualizations", script_name)
    
    try:
        # Run the script
        result = subprocess.run([
            sys.executable, script_path, rdf_file
        ], capture_output=False, text=True, timeout=300)  # 5 minute timeout
        
        if result.returncode == 0:
            print(f"✅ {script_name} completed successfully!")
            return True
        else:
            print(f"❌ {script_name} failed with return code {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {script_name} timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"❌ Error running {script_name}: {str(e)}")
        return False

def main():
    """Main function to run all visualizations"""
    if len(sys.argv) != 2:
        print("Usage: python run_all_visualizations.py <rdf_file_path>")
        print("Example: python run_all_visualizations.py data\\knowledge_graph\\enhanced_unified_klm_hub_kg_20250604_123456.ttl")
        return 1
    
    rdf_file = sys.argv[1]
    
    # Check if RDF file exists
    if not os.path.exists(rdf_file):
        print(f"❌ RDF file not found: {rdf_file}")
        print("\nPlease run the enhanced unified knowledge graph builder first:")
        print("python scripts\\Unified_KG\\enhanced_unified_kg_builder.py --arcgis-dir data\\ArcGIS_Hub")
        return 1
    
    # Check if we're in the correct directory (project root)
    if not os.path.exists("scripts\\Visualizations"):
        print("❌ Visualization scripts directory not found!")
        print("Please run this script from the project root directory:")
        print("cd C:\\Users\\20211062\\GitHub\\2AMD20-Group-10")
        return 1
    
    print("🎨 KLM Hub Analysis - Enhanced Poster Visualization Suite")
    print("=" * 60)
    print(f"📁 Using RDF file: {rdf_file}")
    print(f"📁 Output directory: poster_visualizations\\")
    print(f"📁 Scripts location: scripts\\Visualizations\\")
    
    # Check if individual scripts exist
    scripts = [
        ("hub_ranking_chart.py", "Main Hub Ranking Chart"),
        ("hub_scatter_plot.py", "Hub Analysis Scatter Plots"), 
        ("hub_summary_stats.py", "Summary Statistics Infographic"),
        ("hub_network_viz.py", "Network Connectivity Analysis"),
        ("hub_comparison_table.py", "Comparison Table")
    ]
    
    # Verify all scripts exist
    missing_scripts = []
    for script_file, _ in scripts:
        script_path = os.path.join("scripts", "Visualizations", script_file)
        if not os.path.exists(script_path):
            missing_scripts.append(script_file)
    
    if missing_scripts:
        print(f"\n❌ Missing visualization scripts:")
        for script in missing_scripts:
            print(f"   • {script}")
        print(f"\nPlease ensure all visualization scripts are saved in scripts\\Visualizations\\")
        return 1
    
    # Track results
    results = {}
    start_time = time.time()
    
    # Run each script
    for script_file, description in scripts:
        print(f"\n⏳ Starting: {description}")
        success = run_script(script_file, rdf_file)
        results[script_file] = success
        
        if success:
            print(f"✅ Completed: {description}")
        else:
            print(f"❌ Failed: {description}")
        
        # Brief pause between scripts
        time.sleep(2)
    
    # Summary
    total_time = time.time() - start_time
    successful = sum(results.values())
    total = len(results)
    
    print(f"\n{'='*60}")
    print(f"🎉 VISUALIZATION GENERATION COMPLETE")
    print(f"{'='*60}")
    print(f"⏱️  Total time: {total_time:.1f} seconds")
    print(f"✅ Successful: {successful}/{total}")
    print(f"❌ Failed: {total - successful}/{total}")
    
    print(f"\n📊 RESULTS SUMMARY:")
    for script, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"  {script:<25} {status}")
    
    if successful > 0:
        print(f"\n📁 Generated files are in: poster_visualizations\\")
        print(f"\n🎯 POSTER RECOMMENDATIONS:")
        if results.get("hub_ranking_chart.py", False):
            print("  • Use hub_ranking_chart.png as main centerpiece")
        if results.get("hub_summary_stats.py", False):
            print("  • Include hub_summary_infographic.png for key statistics")
        if results.get("hub_network_viz.py", False):
            print("  • Add hub_network_analysis.png to show strategic value")
        if results.get("hub_scatter_plot.py", False):
            print("  • Use hub_scatter_analysis.png for detailed analysis")
        if results.get("hub_comparison_table.py", False):
            print("  • Reference hub_comparison_table.png for full comparison")
        
        print(f"\n📋 NEXT STEPS:")
        print("  1. Review all generated visualizations")
        print("  2. Select best charts for your poster layout")
        print("  3. Ensure all images are high-resolution (300 DPI)")
        print("  4. Consider the poster size and readability")
        print("  5. Enhanced visualizations now include ArcGIS infrastructure data!")
        
    if successful < total:
        print(f"\n⚠️  TROUBLESHOOTING:")
        print("  • Check that all required packages are installed:")
        print("    pip install matplotlib seaborn pandas networkx rdflib")
        print("  • Verify the RDF file contains valid data")
        print("  • Check the console output above for specific errors")
        print("  • Ensure you're running from the project root directory")
        print("  • Verify all visualization scripts are in scripts\\Visualizations\\")
    
    return 0 if successful == total else 1

if __name__ == "__main__":
    exit(main())