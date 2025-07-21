
import asyncio
import logging

from pkg.big_query.services.table_search import BQSearchTable
from pkg.big_query.services.table_search_test import load_sample_data
from pkg.agentic.service.agent_interface import BQAgenticDataCatalogueInterface


async def demo_bq_agentic_workflow():
    
    # Initialize the search engine with sample data
    search_engine = BQSearchTable()
    sample_tables = load_sample_data()
    
    for table in sample_tables:
        search_engine.add_table(table)
    
    # Initialize the interface
    interface = BQAgenticDataCatalogueInterface(search_engine)
    
    print("🤖 BigQuery Agentic AI Data Catalogue Demo")
    print("=" * 50)
    
    # Demo queries
    demo_queries = [
        "Where can I find daily campaign performance data?",
        "Show me tables with sales information",
        "What's the schema of marketing tables?", 
        "I need metadata for analytics tables",
        "Find tables related to user behavior"
    ]
    
    for query in demo_queries:
        print(f"\n Query: '{query}'")
        print("─" * 30)
        
        response = await interface.chat(query)
        print(f" Response: {response}")
        
        # Small delay to simulate processing
        await asyncio.sleep(1)
    
    # Show diagnostics
    print("\n System Diagnostics:")
    diagnostics = interface.get_agent_diagnostics()
    for key, value in diagnostics.items():
        print(f"  {key}: {value}")


# =====================================================
# MAIN ENTRY POINT
# =====================================================

def main():
    """Main entry point for the BigQuery Agentic AI system"""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='BigQuery Agentic AI Data Catalogue')
    parser.add_argument('mode', nargs='?', default='cli', 
                       choices=['cli', 'demo'], 
                       help='Run mode: cli (interactive) or demo (demonstration)')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Starting BigQuery Agentic AI Data Catalogue...")
    print(f"Mode: {args.mode}")
    print(f"Log Level: {args.log_level}")
    
    # Initialize search engine with sample data
    search_engine = BQSearchTable()
    sample_tables = load_sample_data()
    
    print(f"Loading {len(sample_tables)} sample tables...")
    for table in sample_tables:
        search_engine.add_table(table)
    
    # Initialize interface
    interface = BQAgenticDataCatalogueInterface(search_engine)
    
    if args.mode == 'demo':
        # Run demo
        print("Running demonstration mode...")
        asyncio.run(demo_bq_agentic_workflow())
    else:
        # Run interactive CLI
        print("Starting interactive CLI mode...")
        asyncio.run(interface.run_cli())

if __name__ == "__main__":
    main()

