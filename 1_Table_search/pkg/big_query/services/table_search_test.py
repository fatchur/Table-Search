
import os 
from typing import List, Dict

current_directory = os.getcwd()
print("Current Directory:", current_directory)

from domains.models.bigquery_table_info import BQTableInfo
from pkg.big_query.services.table_search import BQSearchTable


def load_sample_data() -> List[BQTableInfo]:

    sample_tables = [
        BQTableInfo(
            dataset="marketing",
            table_name="daily_campaign_performance",
            description="Daily performance metrics for marketing campaigns including impressions, clicks, conversions and spend",
            columns=[
                {"name": "date", "description": "Campaign date"},
                {"name": "campaign_id", "description": "Unique campaign identifier"},
                {"name": "impressions", "description": "Number of ad impressions"},
                {"name": "clicks", "description": "Number of clicks"},
                {"name": "conversions", "description": "Number of conversions"},
                {"name": "spend", "description": "Campaign spend amount"}
            ],
            tags=["marketing", "campaign", "daily", "performance"],
            last_modified="2024-01-15",
            row_count=50000
        ),
        BQTableInfo(
            dataset="marketing",
            table_name="campaign_planning_data",
            description="Campaign planning information including budget allocation, target audience, and planned activities",
            columns=[
                {"name": "campaign_id", "description": "Campaign identifier"},
                {"name": "planned_budget", "description": "Planned campaign budget"},
                {"name": "target_audience", "description": "Target audience description"},
                {"name": "start_date", "description": "Campaign start date"},
                {"name": "end_date", "description": "Campaign end date"}
            ],
            tags=["marketing", "campaign", "planning", "budget"],
            last_modified="2024-01-10",
            row_count=1200
        ),
        BQTableInfo(
            dataset="sales",
            table_name="daily_sales_summary",
            description="Daily sales summary with revenue, units sold, and customer metrics",
            columns=[
                {"name": "date", "description": "Sales date"},
                {"name": "revenue", "description": "Total revenue"},
                {"name": "units_sold", "description": "Number of units sold"},
                {"name": "customers", "description": "Number of unique customers"}
            ],
            tags=["sales", "daily", "revenue", "summary"],
            last_modified="2024-01-14",
            row_count=30000
        ),
        BQTableInfo(
            dataset="analytics",
            table_name="user_behavior_daily",
            description="Daily user behavior analytics including page views, session duration, and user actions",
            columns=[
                {"name": "date", "description": "Analytics date"},
                {"name": "user_id", "description": "User identifier"},
                {"name": "page_views", "description": "Number of page views"},
                {"name": "session_duration", "description": "Session duration in seconds"}
            ],
            tags=["analytics", "user", "behavior", "daily"],
            last_modified="2024-01-15",
            row_count=100000
        ),
        BQTableInfo(
            dataset="finance",
            table_name="budget_planning",
            description="Budget planning data for different departments and projects",
            columns=[
                {"name": "department", "description": "Department name"},
                {"name": "project", "description": "Project name"},
                {"name": "planned_budget", "description": "Planned budget amount"},
                {"name": "actual_spend", "description": "Actual spend amount"}
            ],
            tags=["finance", "budget", "planning", "department"],
            last_modified="2024-01-12",
            row_count=500
        ),
        BQTableInfo(
            dataset="marketing",
            table_name="campaign_metadata",
            description="Metadata for marketing campaigns including campaign type, channels, and objectives",
            columns=[
                {"name": "campaign_id", "description": "Campaign identifier"},
                {"name": "campaign_type", "description": "Type of campaign"},
                {"name": "channels", "description": "Marketing channels used"},
                {"name": "objectives", "description": "Campaign objectives"}
            ],
            tags=["marketing", "campaign", "metadata", "channels"],
            last_modified="2024-01-13",
            row_count=800
        ),
        BQTableInfo(
            dataset="product",
            table_name="daily_product_metrics",
            description="Daily product usage metrics and feature adoption rates",
            columns=[
                {"name": "date", "description": "Metrics date"},
                {"name": "product_id", "description": "Product identifier"},
                {"name": "active_users", "description": "Daily active users"},
                {"name": "feature_usage", "description": "Feature usage statistics"}
            ],
            tags=["product", "daily", "metrics", "usage"],
            last_modified="2024-01-15",
            row_count=25000
        ),
        BQTableInfo(
            dataset="customer",
            table_name="customer_journey_data",
            description="Customer journey data tracking touchpoints and conversion paths",
            columns=[
                {"name": "customer_id", "description": "Customer identifier"},
                {"name": "touchpoint", "description": "Customer touchpoint"},
                {"name": "timestamp", "description": "Touchpoint timestamp"},
                {"name": "conversion_flag", "description": "Conversion indicator"}
            ],
            tags=["customer", "journey", "touchpoint", "conversion"],
            last_modified="2024-01-11",
            row_count=150000
        ),
        BQTableInfo(
            dataset="operations",
            table_name="daily_operations_report",
            description="Daily operations report including system uptime, error rates, and performance metrics",
            columns=[
                {"name": "date", "description": "Report date"},
                {"name": "system_uptime", "description": "System uptime percentage"},
                {"name": "error_rate", "description": "Error rate percentage"},
                {"name": "avg_response_time", "description": "Average response time"}
            ],
            tags=["operations", "daily", "system", "performance"],
            last_modified="2024-01-15",
            row_count=365
        ),
        BQTableInfo(
            dataset="marketing",
            table_name="campaign_daily_plan",
            description="Daily campaign execution plan with scheduled activities and resource allocation",
            columns=[
                {"name": "date", "description": "Plan date"},
                {"name": "campaign_id", "description": "Campaign identifier"},
                {"name": "scheduled_activities", "description": "Scheduled campaign activities"},
                {"name": "resource_allocation", "description": "Resource allocation details"}
            ],
            tags=["marketing", "campaign", "daily", "plan", "execution"],
            last_modified="2024-01-14",
            row_count=5000
        )
    ]
    
    return sample_tables



def main():
    search_engine = BQSearchTable()
    sample_tables = load_sample_data()
    
    for table in sample_tables:
        search_engine.add_table(table)
    
    print("BigQuery Data Dictionary Search Engine")
    print("=" * 50)
    

    test_queries = [
        "Where I can find the daily campaign plan data?",
        "Show me tables with sales information",
        "I need user behavior analytics",
        "Find tables about budget planning",
        "What tables contain campaign performance metrics?"
    ]
    
    for query in test_queries:
        print(f"\nQuery: '{query}'")
        print("-" * 30)
        
        results = search_engine.search(query)
        
        if results:
            for i, result in enumerate(results, 1):
                print(f"{i}. {result['table_name']}")
                print(f"   Description: {result['description']}")
                print(f"   Relevance Score: {result['relevance_score']}")
                print(f"   Matched Keywords: {', '.join(result['matched_keywords'])}")
                print(f"   Rows: {result['row_count']:,} | Columns: {result['columns']}")
                print(f"   Tags: {', '.join(result['tags'])}")
                print()
        else:
            print("No relevant tables found.")
    
    # Interactive mode
    print("\n" + "=" * 50)
    print("Interactive Mode - Enter your queries (type 'quit' to exit):")
    
    while True:
        query = input("\nEnter your query: ").strip()
        
        if query.lower() in ['quit', 'exit', 'q']:
            break
        
        if not query:
            continue
        
        results = search_engine.search(query)
        
        if results:
            print(f"\nFound {len(results)} relevant tables:")
            for i, result in enumerate(results, 1):
                print(f"\n{i}. {result['table_name']}")
                print(f"   Description: {result['description']}")
                print(f"   Relevance Score: {result['relevance_score']}")
                print(f"   Last Modified: {result['last_modified']}")
                print(f"   Rows: {result['row_count']:,}")
        else:
            print("No relevant tables found for your query.")

if __name__ == "__main__":
    main()