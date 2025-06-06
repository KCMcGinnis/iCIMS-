import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from typing import Dict, List, Optional
import time

class ICIMSConnector:
    """
    Connector class for iCIMS API to extract recruitment data
    """
    
    def __init__(self, base_url: str, username: str, password: str, customer_id: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.customer_id = customer_id
        self.session = requests.Session()
        self.auth_token = None
        
    def authenticate(self):
        """Authenticate with iCIMS API"""
        auth_url = f"{self.base_url}/connect/authorize"
        
        auth_data = {
            'username': self.username,
            'password': self.password,
            'customerid': self.customer_id
        }
        
        try:
            response = self.session.post(auth_url, json=auth_data)
            response.raise_for_status()
            
            auth_response = response.json()
            self.auth_token = auth_response.get('access_token')
            
            # Set authorization header for future requests
            self.session.headers.update({
                'Authorization': f'Bearer {self.auth_token}',
                'Content-Type': 'application/json'
            })
            
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Authentication failed: {e}")
            return False
    
    def get_jobs(self, status: str = 'open', limit: int = 100) -> List[Dict]:
        """Extract job/position data from iCIMS"""
        if not self.auth_token:
            if not self.authenticate():
                return []
        
        jobs_url = f"{self.base_url}/connect/jobs"
        
        params = {
            'status': status,
            'limit': limit,
            'fields': 'id,title,department,location,status,dateposted,dateclosed,recruiter'
        }
        
        try:
            response = self.session.get(jobs_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get('jobs', [])
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching jobs: {e}")
            return []
    
    def get_candidates(self, job_id: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Extract candidate data from iCIMS"""
        if not self.auth_token:
            if not self.authenticate():
                return []
        
        candidates_url = f"{self.base_url}/connect/candidates"
        
        params = {
            'limit': limit,
            'fields': 'id,firstname,lastname,email,phone,status,source,dateadded,jobid,recruiter'
        }
        
        if job_id:
            params['jobid'] = job_id
        
        try:
            response = self.session.get(candidates_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get('candidates', [])
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching candidates: {e}")
            return []
    
    def get_workflow_steps(self, job_id: str) -> List[Dict]:
        """Get workflow steps for a specific job"""
        if not self.auth_token:
            if not self.authenticate():
                return []
        
        workflow_url = f"{self.base_url}/connect/jobs/{job_id}/workflow"
        
        try:
            response = self.session.get(workflow_url)
            response.raise_for_status()
            
            data = response.json()
            return data.get('steps', [])
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching workflow steps: {e}")
            return []

class RecruitmentDashboard:
    """
    Dashboard class to process and visualize recruitment metrics
    """
    
    def __init__(self, icims_connector: ICIMSConnector):
        self.connector = icims_connector
        self.jobs_df = None
        self.candidates_df = None
        
    def extract_data(self):
        """Extract all necessary data from iCIMS"""
        print("Extracting jobs data...")
        jobs_data = self.connector.get_jobs(limit=500)
        self.jobs_df = pd.DataFrame(jobs_data)
        
        print("Extracting candidates data...")
        candidates_data = self.connector.get_candidates(limit=1000)
        self.candidates_df = pd.DataFrame(candidates_data)
        
        # Data preprocessing
        self._preprocess_data()
        
    def _preprocess_data(self):
        """Clean and preprocess the extracted data"""
        if not self.jobs_df.empty:
            # Convert date columns
            date_columns = ['dateposted', 'dateclosed']
            for col in date_columns:
                if col in self.jobs_df.columns:
                    self.jobs_df[col] = pd.to_datetime(self.jobs_df[col], errors='coerce')
            
            # Calculate days to fill
            self.jobs_df['days_to_fill'] = (
                self.jobs_df['dateclosed'] - self.jobs_df['dateposted']
            ).dt.days
            
        if not self.candidates_df.empty:
            # Convert date columns
            if 'dateadded' in self.candidates_df.columns:
                self.candidates_df['dateadded'] = pd.to_datetime(
                    self.candidates_df['dateadded'], errors='coerce'
                )
            
            # Add week/month columns for time-series analysis
            self.candidates_df['week'] = self.candidates_df['dateadded'].dt.to_period('W')
            self.candidates_df['month'] = self.candidates_df['dateadded'].dt.to_period('M')
    
    def calculate_metrics(self) -> Dict:
        """Calculate key recruitment metrics"""
        metrics = {}
        
        if not self.jobs_df.empty:
            # Job metrics
            metrics['total_jobs'] = len(self.jobs_df)
            metrics['open_jobs'] = len(self.jobs_df[self.jobs_df['status'] == 'open'])
            metrics['closed_jobs'] = len(self.jobs_df[self.jobs_df['status'] == 'closed'])
            
            # Average time to fill
            filled_jobs = self.jobs_df[self.jobs_df['days_to_fill'].notna()]
            metrics['avg_time_to_fill'] = filled_jobs['days_to_fill'].mean() if not filled_jobs.empty else 0
            
        if not self.candidates_df.empty:
            # Candidate metrics
            metrics['total_candidates'] = len(self.candidates_df)
            metrics['candidates_this_month'] = len(
                self.candidates_df[
                    self.candidates_df['dateadded'] >= datetime.now() - timedelta(days=30)
                ]
            )
            
            # Source effectiveness
            source_counts = self.candidates_df['source'].value_counts()
            metrics['top_source'] = source_counts.index[0] if not source_counts.empty else 'N/A'
            
        return metrics
    
    def create_position_metrics_chart(self):
        """Create chart showing metrics by position"""
        if self.jobs_df.empty:
            return go.Figure()
        
        # Group by job title and calculate metrics
        position_metrics = self.jobs_df.groupby('title').agg({
            'id': 'count',
            'days_to_fill': 'mean',
            'status': lambda x: (x == 'open').sum()
        }).reset_index()
        
        position_metrics.columns = ['Position', 'Total_Jobs', 'Avg_Days_to_Fill', 'Open_Jobs']
        position_metrics = position_metrics.head(15)  # Top 15 positions
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Jobs by Position', 'Average Days to Fill'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Total jobs bar chart
        fig.add_trace(
            go.Bar(
                x=position_metrics['Position'],
                y=position_metrics['Total_Jobs'],
                name='Total Jobs',
                marker_color='lightblue'
            ),
            row=1, col=1
        )
        
        # Average days to fill
        fig.add_trace(
            go.Bar(
                x=position_metrics['Position'],
                y=position_metrics['Avg_Days_to_Fill'],
                name='Avg Days to Fill',
                marker_color='lightcoral'
            ),
            row=1, col=2
        )
        
        fig.update_layout(
            title_text="Position Metrics Overview",
            showlegend=False,
            height=500
        )
        
        fig.update_xaxes(tickangle=45)
        
        return fig
    
    def create_recruiter_metrics_chart(self):
        """Create chart showing metrics by recruiter"""
        if self.jobs_df.empty or self.candidates_df.empty:
            return go.Figure()
        
        # Recruiter job metrics
        recruiter_jobs = self.jobs_df.groupby('recruiter').agg({
            'id': 'count',
            'days_to_fill': 'mean'
        }).reset_index()
        recruiter_jobs.columns = ['Recruiter', 'Total_Jobs', 'Avg_Days_to_Fill']
        
        # Recruiter candidate metrics
        recruiter_candidates = self.candidates_df.groupby('recruiter').size().reset_index()
        recruiter_candidates.columns = ['Recruiter', 'Total_Candidates']
        
        # Merge metrics
        recruiter_metrics = pd.merge(recruiter_jobs, recruiter_candidates, on='Recruiter', how='outer')
        recruiter_metrics = recruiter_metrics.fillna(0).head(10)  # Top 10 recruiters
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Jobs by Recruiter', 'Candidates by Recruiter', 
                          'Average Days to Fill', 'Efficiency Score'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Jobs by recruiter
        fig.add_trace(
            go.Bar(x=recruiter_metrics['Recruiter'], y=recruiter_metrics['Total_Jobs'],
                  name='Jobs', marker_color='skyblue'),
            row=1, col=1
        )
        
        # Candidates by recruiter
        fig.add_trace(
            go.Bar(x=recruiter_metrics['Recruiter'], y=recruiter_metrics['Total_Candidates'],
                  name='Candidates', marker_color='lightgreen'),
            row=1, col=2
        )
        
        # Average days to fill
        fig.add_trace(
            go.Bar(x=recruiter_metrics['Recruiter'], y=recruiter_metrics['Avg_Days_to_Fill'],
                  name='Days to Fill', marker_color='salmon'),
            row=2, col=1
        )
        
        # Efficiency score (candidates per job)
        efficiency = recruiter_metrics['Total_Candidates'] / (recruiter_metrics['Total_Jobs'] + 1)
        fig.add_trace(
            go.Bar(x=recruiter_metrics['Recruiter'], y=efficiency,
                  name='Efficiency', marker_color='gold'),
            row=2, col=2
        )
        
        fig.update_layout(
            title_text="Recruiter Performance Metrics",
            showlegend=False,
            height=700
        )
        
        fig.update_xaxes(tickangle=45)
        
        return fig
    
    def create_timeline_chart(self):
        """Create timeline chart showing recruitment activity"""
        if self.candidates_df.empty:
            return go.Figure()
        
        # Weekly candidate additions
        weekly_data = self.candidates_df.groupby('week').size().reset_index()
        weekly_data['week_str'] = weekly_data['week'].astype(str)
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=weekly_data['week_str'],
            y=weekly_data[0],
            mode='lines+markers',
            name='Candidates Added',
            line=dict(color='blue', width=2),
            marker=dict(size=6)
        ))
        
        fig.update_layout(
            title='Candidate Addition Timeline (Weekly)',
            xaxis_title='Week',
            yaxis_title='Number of Candidates',
            height=400
        )
        
        return fig

def create_streamlit_dashboard():
    """Create Streamlit dashboard interface"""
    st.set_page_config(page_title="Recruitment Dashboard", layout="wide")
    
    st.title("🎯 Recruitment Analytics Dashboard")
    st.markdown("---")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        
        base_url = st.text_input("iCIMS Base URL", placeholder="https://your-company.icims.com")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        customer_id = st.text_input("Customer ID")
        
        if st.button("Connect & Refresh Data"):
            if all([base_url, username, password, customer_id]):
                with st.spinner("Connecting to iCIMS and extracting data..."):
                    connector = ICIMSConnector(base_url, username, password, customer_id)
                    dashboard = RecruitmentDashboard(connector)
                    
                    try:
                        dashboard.extract_data()
                        st.session_state['dashboard'] = dashboard
                        st.success("Data extracted successfully!")
                    except Exception as e:
                        st.error(f"Error extracting data: {e}")
            else:
                st.error("Please fill in all configuration fields")
    
    # Main dashboard
    if 'dashboard' in st.session_state:
        dashboard = st.session_state['dashboard']
        metrics = dashboard.calculate_metrics()
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Jobs", metrics.get('total_jobs', 0))
        with col2:
            st.metric("Open Positions", metrics.get('open_jobs', 0))
        with col3:
            st.metric("Total Candidates", metrics.get('total_candidates', 0))
        with col4:
            st.metric("Avg. Time to Fill", f"{metrics.get('avg_time_to_fill', 0):.1f} days")
        
        st.markdown("---")
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Position Metrics")
            position_chart = dashboard.create_position_metrics_chart()
            st.plotly_chart(position_chart, use_container_width=True)
        
        with col2:
            st.subheader("Recruitment Timeline")
            timeline_chart = dashboard.create_timeline_chart()
            st.plotly_chart(timeline_chart, use_container_width=True)
        
        st.subheader("Recruiter Performance")
        recruiter_chart = dashboard.create_recruiter_metrics_chart()
        st.plotly_chart(recruiter_chart, use_container_width=True)
        
        # Data tables
        st.subheader("Recent Data")
        
        tab1, tab2 = st.tabs(["Jobs", "Candidates"])
        
        with tab1:
            if not dashboard.jobs_df.empty:
                st.dataframe(dashboard.jobs_df.head(20), use_container_width=True)
        
        with tab2:
            if not dashboard.candidates_df.empty:
                st.dataframe(dashboard.candidates_df.head(20), use_container_width=True)
    
    else:
        st.info("Please configure your iCIMS connection in the sidebar to get started.")
        
        # Demo section
        st.subheader("Dashboard Features")
        st.markdown("""
        This dashboard provides:
        
        **Key Metrics:**
        - Total jobs and open positions
        - Candidate volume and trends
        - Average time to fill positions
        - Recruiter performance metrics
        
        **Visualizations:**
        - Position-wise job distribution and time to fill
        - Recruiter performance comparison
        - Candidate addition timeline
        - Source effectiveness analysis
        
        **Data Tables:**
        - Recent jobs with status and metrics
        - Candidate pipeline with sources and stages
        """)

python

# Run the dashboard
create_streamlit_dashboard()
