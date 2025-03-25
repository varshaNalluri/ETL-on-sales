import streamlit as st
import pandas as pd
import plotly.express as px

df = pd.read_csv(r"C:\Users\Nalluri_Sri_varsha\OneDrive - Dell Technologies\Desktop\LLM\ETL\transformed_data.csv")

st.title("Warehouse and Retail Sales Dashboard")
st.write('Visualizing the sales and profit margin.')

supplier_list = df['SUPPLIER'].dropna().unique()
selected_supplier = st.sidebar.selectbox('Select a Supplier:', supplier_list)

filtered_df = df[df['SUPPLIER'] == selected_supplier]

total_sales = filtered_df['TOTAL SALES'].sum()
avg_profit_margin = filtered_df['PROFIT MARGIN'].mean()

st.metric(label="Total Sales", value=f"${total_sales:,.2f}")
st.metric(label="Average Profit Margin", value=f"{avg_profit_margin:.2%}")

st.subheader('Sales Over Time')
st.line_chart(filtered_df[['MONTH', 'TOTAL SALES']].set_index('MONTH'))

# --- Bar Chart: Profit Margins ---
st.subheader("Profit Margins by Supplier")
fig1 = px.bar(filtered_df, x='SUPPLIER', y='PROFIT MARGIN', color='SUPPLIER', title="Profit Margins by Supplier")
st.plotly_chart(fig1)

# --- Pie Chart: Item Type Distribution ---
st.subheader("Item Type Distribution")
item_type_distribution = filtered_df['ITEM TYPE'].value_counts().reset_index()
item_type_distribution.columns = ['Item Type', 'Count']
fig2 = px.pie(item_type_distribution, values='Count', names='Item Type', title='Item Type Distribution')
st.plotly_chart(fig2)

st.subheader('Sales Data')
st.write(filtered_df)


