"""
CRM Data Simulation for ABXplore A/B Testing
B2B SaaS company testing new lead onboarding process launched June 1st, 2024.
VERSION: Saves to both DATABASE and CSV files in raw_data folder
"""

import pandas as pd
import numpy as np
import sqlite3
from faker import Faker
from datetime import datetime, timedelta
import random
import uuid
import os
from pathlib import Path

class HybridCRMGenerator:

    
    def __init__(self, seed=42):
        self.fake = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
        random.seed(seed)
        
        # Real business scenario parameters
        self.total_leads = 10000
        self.test_start_date = datetime(2024, 6, 1)  # When new process launched
        self.data_start_date = datetime(2024, 1, 1)  # CRM data starts here
        
        # Realistic CRM structure
        self.regions = ['North America', 'Europe', 'Asia Pacific', 'Latin America']
        self.channels = ['Website', 'Google Ads', 'LinkedIn', 'Referral', 'Cold Email', 'Trade Show', 'Webinar']
        self.industries = ['Healthcare', 'Finance', 'Technology', 'Manufacturing', 'Retail', 'Education']
        self.contact_types = ['Email', 'Phone Call', 'LinkedIn Message', 'Demo Request']
        self.response_types = ['Responded', 'No Response', 'Interested', 'Not Interested', 'Callback Requested']
        self.stages = ['New', 'Contacted', 'Qualified', 'Demo Scheduled', 'Proposal Sent', 'Closed Won', 'Closed Lost']
        
        # Database and CSV setup
        self.db_dir = Path('./db')
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.db_dir / 'abxplore.db'

        self.csv_dir = Path('./raw_data')
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_complete_dataset(self):
        print("🏢 Generating dataset...")
        print("📋 Scenario: B2B SaaS company testing new lead onboarding process")
        print(f"📅 New process launched: {self.test_start_date.strftime('%Y-%m-%d')}")
        
        # Generate realistic CRM tables only
        leads_df = self._generate_leads()
        
        # Add test group logic FIRST (needed for other tables)
        leads_df = self._add_test_group_logic(leads_df)
        
        # Generate in logical order with shared contacted_leads list
        contact_events_df, contacted_lead_ids = self._generate_contact_events(leads_df)
        funnel_stages_df = self._generate_funnel_stages(leads_df, contacted_lead_ids)
        outcomes_df = self._generate_outcomes(leads_df, funnel_stages_df)
        
        # Introduce realistic data quality issues
        leads_df = self._introduce_data_quality_issues_leads(leads_df)
        contact_events_df = self._introduce_data_quality_issues_contacts(contact_events_df)
        funnel_stages_df = self._introduce_data_quality_issues_funnel(funnel_stages_df)
        outcomes_df = self._introduce_data_quality_issues_outcomes(outcomes_df)
        
        # Save to BOTH database and CSV files
        self._save_to_database(leads_df, contact_events_df, funnel_stages_df, outcomes_df)
        self._save_to_csv(leads_df, contact_events_df, funnel_stages_df, outcomes_df)
        
        # Show proper A/B test context  
        control_count = len(leads_df[leads_df['group'] == 'control'])
        test_count = len(leads_df[leads_df['group'] == 'test'])
        
        print(f"   Total leads generated: {len(leads_df):,}")
        print(f"   ├── Control group: {control_count:,} leads")
        print(f"   └── Test group: {test_count:,} leads")
        print(f"   └── Test group: {test_count:,} leads")
        print(f"📊 Total Control: {control_count:,} | Total Test: {test_count:,}")
        print(f"✅ Generated {len(leads_df):,} leads with realistic CRM data")
        print(f"💾 Data saved to database: {self.db_path}")
        print(f"📁 Raw CSV files saved to: {self.csv_dir}")
        
        return {
            'leads': leads_df,
            'contact_events': contact_events_df,
            'funnel_stages': funnel_stages_df,
            'outcomes': outcomes_df
        }
    
    def _generate_leads(self):
        """Generate realistic lead data as found in CRM systems."""
        leads = []
        
        for i in range(self.total_leads):
            # Random lead creation date across the year
            created_at = self.fake.date_time_between(
                start_date=self.data_start_date,
                end_date=datetime(2024, 12, 31)
            )
            
            # Business day bias (85% on weekdays)
            if created_at.weekday() >= 5 and random.random() < 0.85:
                created_at = created_at - timedelta(days=random.randint(1, 2))
            
            # Company size distribution
            company_size = np.random.choice(['Small', 'Medium', 'Large', 'Enterprise'], 
                                          p=[0.4, 0.3, 0.2, 0.1])
            
            # Revenue correlates with company size
            size_multipliers = {'Small': 0.4, 'Medium': 0.7, 'Large': 1.0, 'Enterprise': 2.2}
            base_revenue = np.random.lognormal(15, 1.2) * size_multipliers[company_size]
            
            lead = {
                'lead_id': str(uuid.uuid4()),
                'company_name': self.fake.company(),
                'contact_email': self.fake.email(),
                'contact_phone': self.fake.phone_number() if random.random() > 0.15 else None,
                'industry': np.random.choice(self.industries),
                'region': np.random.choice(self.regions),
                'source_channel': np.random.choice(self.channels),
                'company_size': company_size,
                'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'annual_revenue': base_revenue
            }
            
            leads.append(lead)
        
        return pd.DataFrame(leads)
    
    def _add_test_group_logic(self, leads_df):
        """Add test group assignment - PROPER A/B TEST: Concurrent control and test groups."""
        # FIXED: Real A/B testing with concurrent groups (not sequential time periods)
        # Both groups run simultaneously from June 1st onwards
        
        # Filter to only leads from June 1st onwards (when A/B test started)
        test_period_leads = leads_df[pd.to_datetime(leads_df['created_at']) >= self.test_start_date].copy()
        pre_test_leads = leads_df[pd.to_datetime(leads_df['created_at']) < self.test_start_date].copy()
        
        # Pre-test period: all control (historical baseline)
        pre_test_leads['group'] = 'control'
        pre_test_leads['assigned_at'] = pre_test_leads['created_at'].apply(
            lambda x: pd.to_datetime(x).strftime('%Y-%m-%d')
        )
        
        # A/B test period: Random 50/50 split (proper concurrent testing)
        np.random.seed(42)  # Reproducible results
        random_assignment = np.random.choice(['control', 'test'], 
                                           size=len(test_period_leads), 
                                           p=[0.5, 0.5])
        test_period_leads['group'] = random_assignment
        test_period_leads['assigned_at'] = test_period_leads['created_at'].apply(
            lambda x: pd.to_datetime(x).strftime('%Y-%m-%d')
        )
        
        # Combine periods
        leads_df = pd.concat([pre_test_leads, test_period_leads], ignore_index=True)
        
        return leads_df
    
    def _generate_contact_events(self, leads_df):
        """Generate realistic contact events with business patterns."""
        events = []
        contacted_lead_ids = set()  # Track which leads were contacted
        
        for _, lead in leads_df.iterrows():
            lead_start = pd.to_datetime(lead['created_at'])
            
            # Use actual group assignment (not date-based logic)
            is_test_group = lead['group'] == 'test'
            
            # ADJUSTED RATES TO ACHIEVE 30% CONTROL AND 35%+ TEST CONVERSION
            if is_test_group:
                contact_probability = 0.90  # Test group: 90% contact rate
                avg_contacts = 4.5
                response_boost = 1.4  # Test group gets better responses
            else:
                contact_probability = 0.85  # Control group: 85% contact rate  
                avg_contacts = 4.0
                response_boost = 1.0  # Control group baseline
            
            if random.random() < contact_probability:
                contacted_lead_ids.add(lead['lead_id'])  # Track this lead as contacted
                num_contacts = max(1, int(np.random.poisson(avg_contacts)))
                
                for contact_num in range(num_contacts):
                    # Contact timing: more frequent early, spread out later
                    if contact_num == 0:
                        days_offset = random.randint(0, 2)
                    elif contact_num == 1:
                        days_offset = random.randint(1, 7)
                    else:
                        days_offset = random.randint(7, 30)
                    
                    contact_date = lead_start + timedelta(days=days_offset)
                    
                    # Skip if contact is in the future
                    if contact_date > datetime.now():
                        continue
                    
                    # Contact method varies by sequence
                    if contact_num == 0:
                        contact_type = np.random.choice(['Email', 'Phone Call'], p=[0.7, 0.3])
                    else:
                        contact_type = np.random.choice(self.contact_types)
                    
                    # Response probability (higher for test group)
                    base_response_prob = {
                        'Email': 0.25, 'Phone Call': 0.35, 
                        'LinkedIn Message': 0.15, 'Demo Request': 0.65
                    }.get(contact_type, 0.2)
                    
                    response_prob = base_response_prob * response_boost
                    
                    if random.random() < response_prob:
                        response_type = np.random.choice(['Responded', 'Interested', 'Callback Requested'], 
                                                       p=[0.5, 0.3, 0.2])
                    else:
                        response_type = 'No Response'
                    
                    event = {
                        'event_id': str(uuid.uuid4()),
                        'lead_id': lead['lead_id'],
                        'event_date': contact_date.date() if hasattr(contact_date, 'date') else contact_date,
                        'contact_type': contact_type,
                        'response_type': response_type
                    }
                    
                    events.append(event)
        
        return pd.DataFrame(events), contacted_lead_ids
    
    def _generate_funnel_stages(self, leads_df, contacted_lead_ids):
        """Generate funnel stages ONLY for leads who were contacted (logical progression)."""
        stages = []
        
        # Generate funnel stages ONLY for leads that were actually contacted
        for _, lead in leads_df.iterrows():
            if lead['lead_id'] not in contacted_lead_ids:
                continue  # Skip - not contacted, so no funnel progression
            
            is_test_group = lead['group'] == 'test'
            
            lead_start = pd.to_datetime(lead['created_at'])
            is_test_group = lead['group'] == 'test'
            
            # FINAL RATES TO ACHIEVE EXACT TARGET CONVERSIONS:
            # Control: 85% contact × 65% funnel entry × 54% outcome × 100% conversion = 29.8% ≈ 30%
            # Test: 90% contact × 70% funnel entry × 60% outcome × 100% conversion = 37.8% ≈ 38%
            if is_test_group:
                funnel_entry_rate = 0.70  # 70% of contacted leads enter funnel
                outcome_rate = 0.60  # 60% of funnel leads get outcomes
                conversion_rate = 1.0  # 100% of outcomes convert (all outcomes are conversions)
            else:
                funnel_entry_rate = 0.65  # 65% of contacted leads enter funnel
                outcome_rate = 0.54  # 54% of funnel leads get outcomes  
                conversion_rate = 1.0  # 100% of outcomes convert (all outcomes are conversions)
            
            # Only some contacted leads enter the funnel
            if random.random() > funnel_entry_rate:
                continue  # Skip - contacted but didn't enter funnel
            
            # Standard stage progression for funnel leads
            stage_probabilities = {
                'Contacted': 1.0,   # Already contacted by definition
                'Qualified': 0.90,  # 90% of funnel leads qualify
                'Demo Scheduled': 0.80,  # 80% book demos
                'Proposal Sent': 0.75,   # 75% get proposals
                'Closed Won': outcome_rate   # Outcome rate determines final conversion
            }
            
            current_date = lead_start
            reached_stages = ['New']  # All leads start as new
            
            # Progress through stages
            for stage_order, stage in enumerate(['Contacted', 'Qualified', 'Demo Scheduled', 'Proposal Sent', 'Closed Won'], 1):
                if random.random() < stage_probabilities[stage]:
                    # Calculate when this stage was reached
                    days_to_stage = {
                        'Contacted': np.random.exponential(2),
                        'Qualified': np.random.exponential(7),
                        'Demo Scheduled': np.random.exponential(14),
                        'Proposal Sent': np.random.exponential(21),
                        'Closed Won': np.random.exponential(35)
                    }[stage]
                    
                    stage_date = current_date + timedelta(days=days_to_stage)
                    
                    if stage_date <= datetime.now():
                        reached_stages.append(stage)
                        
                        stage_record = {
                            'stage_id': str(uuid.uuid4()),
                            'lead_id': lead['lead_id'],
                            'stage_name': stage,
                            'stage_date': stage_date.date() if hasattr(stage_date, 'date') else stage_date,
                            'stage_order': stage_order + 1  # +1 because 'New' is order 1
                        }
                        
                        stages.append(stage_record)
                else:
                    break  # Exit funnel at this stage
        
        return pd.DataFrame(stages)
    
    def _generate_outcomes(self, leads_df, funnel_stages_df):
        """Generate outcomes focused on Closed Won leads from funnel."""
        outcomes = []
        
        # Find leads that reached 'Closed Won' stage
        closed_won_leads = funnel_stages_df[funnel_stages_df['stage_name'] == 'Closed Won']['lead_id'].unique()
        
        for lead_id in closed_won_leads:
            lead_info = leads_df[leads_df['lead_id'] == lead_id].iloc[0]
            lead_start = pd.to_datetime(lead_info['created_at'])
            
            # Get the Closed Won stage date
            closed_won_stage = funnel_stages_df[
                (funnel_stages_df['lead_id'] == lead_id) & 
                (funnel_stages_df['stage_name'] == 'Closed Won')
            ].iloc[0]
            
            outcome_date = pd.to_datetime(closed_won_stage['stage_date'])
            days_to_close = (outcome_date - lead_start).days
            
            # Revenue based on company size and test group
            is_test_group = lead_info['group'] == 'test'
            
            size_multipliers = {'Small': 1.0, 'Medium': 1.5, 'Large': 2.5, 'Enterprise': 4.0}
            base_revenue = 25000 * size_multipliers.get(lead_info['company_size'], 1.0)
            
            # Test group gets revenue boost
            if is_test_group:
                revenue_multiplier = np.random.normal(1.3, 0.2)  # 30% boost on average
            else:
                revenue_multiplier = np.random.normal(1.0, 0.2)
            
            final_revenue = max(5000, base_revenue * revenue_multiplier)  # Min $5K deal
            
            outcome = {
                'outcome_id': str(uuid.uuid4()),
                'lead_id': lead_id,
                'converted': 1,  # All these are conversions
                'revenue': final_revenue,
                'outcome_date': outcome_date.date() if hasattr(outcome_date, 'date') else outcome_date,
                'days_to_close': max(1, days_to_close)  # Minimum 1 day
            }
            
            outcomes.append(outcome)
        
        # Add realistic non-converted outcomes (leads that reached funnel but didn't close)
        # Get all leads that entered funnel but didn't reach Closed Won
        funnel_lead_ids = set(funnel_stages_df['lead_id'].unique())
        converted_lead_ids_set = set(closed_won_leads)
        funnel_but_not_converted = funnel_lead_ids - converted_lead_ids_set
        
        # 40% of funnel leads that didn't convert get formal "lost" outcomes  
        # This creates realistic ~75% outcome-to-conversion rate instead of 99%
        for lead_id in funnel_but_not_converted:
            if random.random() < 0.40:  # 40% get formal lost outcomes
                lead_info = leads_df[leads_df['lead_id'] == lead_id].iloc[0]
                lead_start = pd.to_datetime(lead_info['created_at'])
                
                # Random outcome date after lead creation
                days_worked = random.randint(30, 120)  # Worked for 1-4 months
                outcome_date = lead_start + timedelta(days=days_worked)
                
                if outcome_date <= datetime.now():
                    outcome = {
                        'outcome_id': str(uuid.uuid4()),
                        'lead_id': lead_id,
                        'converted': 0,  # Not converted
                        'revenue': 0,
                        'outcome_date': outcome_date.date() if hasattr(outcome_date, 'date') else outcome_date,
                        'days_to_close': days_worked
                    }
                    outcomes.append(outcome)
        
        return pd.DataFrame(outcomes)
    
    def _introduce_data_quality_issues_leads(self, df):
        """Introduce realistic data quality issues that staging models will clean."""
        df = df.copy()
        
        # 1. Test company names (2% of records) - staging will filter these out
        test_indices = df.sample(frac=0.02).index
        test_names = ['Test Company Inc', 'Delete This Company', 'Sample Corp LLC', 'Test Data Corp']
        for idx in test_indices:
            df.loc[idx, 'company_name'] = np.random.choice(test_names)
        
        # 2. Phone numbers with extensions and formatting issues (15% of records with phones)
        phone_mask = df['contact_phone'].notna()
        if phone_mask.sum() > 0:
            phone_indices = df[phone_mask].sample(frac=0.15).index
            extension_formats = [' x123', ' ext 456', ' extension 789', ' x1001']
            for idx in phone_indices:
                df.loc[idx, 'contact_phone'] = df.loc[idx, 'contact_phone'] + np.random.choice(extension_formats)
        
        # 3. Missing regions (5% of records) - staging will set to 'Unknown'
        missing_region_indices = df.sample(frac=0.05).index
        df.loc[missing_region_indices, 'region'] = None
        
        # 4. Company size case variations (8% of records) - staging will standardize
        case_indices = df.sample(frac=0.08).index
        size_variations = {'Small': 'small', 'Medium': 'medium', 'Large': 'large', 'Enterprise': 'enterprise'}
        for idx in case_indices:
            original_size = df.loc[idx, 'company_size']
            if original_size in size_variations:
                df.loc[idx, 'company_size'] = size_variations[original_size]
        
        # 5. Future dates (0.6% of records) - staging will cap to today
        future_indices = df.sample(frac=0.006).index
        future_dates = ['2025-01-15 10:00:00', '2025-02-20 14:30:00', '2025-03-10 09:15:00']
        for idx in future_indices:
            df.loc[idx, 'created_at'] = np.random.choice(future_dates)
        
        # 6. Extreme revenue outliers (1.2% of records) - staging will cap/fix
        outlier_indices = df.sample(frac=0.012).index
        extreme_values = [-500000, 999999999, 50000000, -100000]  # Mix of negative and extreme positive
        for idx in outlier_indices:
            df.loc[idx, 'annual_revenue'] = np.random.choice(extreme_values)
        
        return df
    
    def _introduce_data_quality_issues_contacts(self, df):
        """Introduce realistic data quality issues that staging models will clean."""
        df = df.copy()
        
        # 1. Missing response types (6% of records) - staging will set to 'No Response'
        missing_indices = df.sample(frac=0.06).index
        df.loc[missing_indices, 'response_type'] = None
        
        # 2. Contact type casing and naming variations (12% of records) - staging will standardize
        case_indices = df.sample(frac=0.12).index
        type_variations = {
            'Email': ['email', 'Email'],
            'Phone Call': ['call', 'phone call', 'Phone Call'],
            'LinkedIn Message': ['linkedin', 'LinkedIn', 'LinkedIn Message'],
            'Demo Request': ['demo request']
        }
        
        for idx in case_indices:
            original_type = df.loc[idx, 'contact_type']
            variations = type_variations.get(original_type, [original_type.lower()])
            df.loc[idx, 'contact_type'] = np.random.choice(variations)
        
        # 3. Future event dates (1.2% of records) - staging will cap to today
        future_indices = df.sample(frac=0.012).index
        future_dates = ['2025-02-01', '2025-01-25', '2025-03-15']
        for idx in future_indices:
            df.loc[idx, 'event_date'] = np.random.choice(future_dates)
        
        # 4. Empty string response types (3% of records) - staging will handle
        empty_indices = df.sample(frac=0.03).index
        df.loc[empty_indices, 'response_type'] = ''
        
        return df
    
    def _introduce_data_quality_issues_funnel(self, df):
        """Introduce realistic data quality issues that staging models will clean."""
        df = df.copy()
        
        # 1. Invalid stage orders (2.5% of records) - staging will fix to 1-7 range
        invalid_indices = df.sample(frac=0.025).index
        invalid_orders = [-1, 0, 8, 99, -5]
        for idx in invalid_indices:
            df.loc[idx, 'stage_order'] = np.random.choice(invalid_orders)
        
        # 2. Stage name variations and casing issues (8% of records) - staging will standardize
        inconsistent_indices = df.sample(frac=0.08).index
        name_variations = {
            'New': ['new lead', 'initial', 'new'],
            'Contacted': ['contacted'],
            'Qualified': ['qualified', 'mql', 'marketing qualified'],
            'Demo Scheduled': ['demo', 'demo scheduled'],
            'Proposal Sent': ['proposal', 'quote', 'quote sent'],
            'Closed Won': ['closed won', 'won', 'closed-won'],
            'Closed Lost': ['closed lost', 'lost', 'closed-lost']
        }
        
        for idx in inconsistent_indices:
            if idx < len(df):
                original_name = df.loc[idx, 'stage_name']
                variations = name_variations.get(original_name, [original_name.lower()])
                df.loc[idx, 'stage_name'] = np.random.choice(variations)
        
        # 3. Future stage dates (1% of records) - staging will cap to today
        future_indices = df.sample(frac=0.01).index
        future_dates = ['2025-01-30', '2025-02-15']
        for idx in future_indices:
            df.loc[idx, 'stage_date'] = np.random.choice(future_dates)
        
        # 4. Missing stage orders (1.5% of records) - staging will handle
        missing_order_indices = df.sample(frac=0.015).index
        df.loc[missing_order_indices, 'stage_order'] = None
        
        return df
    
    def _introduce_data_quality_issues_outcomes(self, df):
        """Introduce realistic data quality issues that staging models will clean."""
        df = df.copy()
        
        # 1. Negative revenue values (1.5% of records) - staging will set to 0
        negative_indices = df.sample(frac=0.015).index
        for idx in negative_indices:
            df.loc[idx, 'revenue'] = -abs(df.loc[idx, 'revenue'])
        
        # 2. Extreme revenue outliers (1% of records) - staging will cap to realistic amounts
        extreme_indices = df.sample(frac=0.01).index
        extreme_values = [999999999, 50000000, 2500000]  # Extreme values that staging will cap
        for idx in extreme_indices:
            df.loc[idx, 'revenue'] = np.random.choice(extreme_values)
        
        # 3. Negative days to close (2% of records) - staging will fix to positive
        negative_days_indices = df.sample(frac=0.02).index
        for idx in negative_days_indices:
            df.loc[idx, 'days_to_close'] = -abs(df.loc[idx, 'days_to_close'])
        
        # 4. Inconsistent conversion/revenue logic (1.5% of records) - staging will fix
        inconsistent_indices = df.sample(frac=0.015).index
        for idx in inconsistent_indices:
            # Create logical inconsistencies that staging will fix
            if df.loc[idx, 'converted'] == 1:
                df.loc[idx, 'revenue'] = 0  # Converted but no revenue
            else:
                df.loc[idx, 'revenue'] = 50000  # Not converted but has revenue
        
        # 5. Extreme days to close (1% of records) - staging will cap to 730 days
        extreme_days_indices = df.sample(frac=0.01).index
        extreme_days = [800, 1000, 1500, 999]
        for idx in extreme_days_indices:
            df.loc[idx, 'days_to_close'] = np.random.choice(extreme_days)
        
        # 6. Missing outcome dates for converted leads (0.8% of records) - staging will handle
        missing_date_indices = df.sample(frac=0.008).index
        converted_missing = missing_date_indices[df.loc[missing_date_indices, 'converted'] == 1]
        df.loc[converted_missing, 'outcome_date'] = None
        
        return df
    
    def _save_to_database(self, leads_df, contact_events_df, funnel_stages_df, outcomes_df):
        """Save all dataframes to SQLite database."""
        conn = sqlite3.connect(self.db_path)
        
        # Save to database
        leads_df.to_sql('leads', conn, if_exists='replace', index=False)
        contact_events_df.to_sql('contact_events', conn, if_exists='replace', index=False)
        funnel_stages_df.to_sql('funnel_stages', conn, if_exists='replace', index=False)
        outcomes_df.to_sql('outcomes', conn, if_exists='replace', index=False)
        
        conn.close()
    
    def _save_to_csv(self, leads_df, contact_events_df, funnel_stages_df, outcomes_df):
        """Save all dataframes to CSV files."""
        leads_df.to_csv(self.csv_dir / 'leads.csv', index=False)
        contact_events_df.to_csv(self.csv_dir / 'contact_events.csv', index=False)
        funnel_stages_df.to_csv(self.csv_dir / 'funnel_stages.csv', index=False)
        outcomes_df.to_csv(self.csv_dir / 'outcomes.csv', index=False)


if __name__ == "__main__":
    print("🚀 Generating ABXplore CRM Data with Realistic Quality Issues")
    print("=" * 60)
    
    # Generate data with comprehensive quality issues
    generator = HybridCRMGenerator(seed=42)
    datasets = generator.generate_complete_dataset()
    
    print("\n✅ Data generation complete!")
    print("📊 Data includes realistic quality issues that staging models will clean:")
    print("   • Test company names and phone extensions")
    print("   • Missing regions and inconsistent casing")
    print("   • Future dates and extreme revenue outliers")
    print("   • Contact type variations and missing responses")
    print("   • Stage name inconsistencies and invalid orders")
    print("   • Revenue/conversion logic issues and extreme values")
    print("\n🔧 Now run dbt transformations to see the staging models clean this data!")
