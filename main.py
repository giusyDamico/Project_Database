import pandas as pd

xl_1=pd.ExcelFile('Input Files/tables.xlsx')
list_name_table=xl_1.sheet_names
#Remove the first name (is the sheet given from your file)
list_name_table.remove('tables')
df_messages= xl_1.parse(list_name_table[1])
df_orders= xl_1.parse(list_name_table[0])

# Computing for each order the first message (extracting order_id and time)
first_message_time_per_order = df_messages.groupby('order_id')['message_sent_time'].min().reset_index()

# Merge DataFrame messages and orders
merged_df = pd.merge(df_messages, df_orders, on='order_id', how='inner')

# Extracting first message of courier and customer
first_courier_message = merged_df.loc[merged_df['sender_app_type'].str.startswith('Courier'), ['order_id', 'message_sent_time']].groupby('order_id').min()
first_customer_message = merged_df.loc[merged_df['sender_app_type'].str.startswith('Customer'), ['order_id', 'message_sent_time']].groupby('order_id').min()
# Trasformation of DataFrame
first_courier_message.reset_index(inplace=True)
first_courier_message.rename(columns={'index': 'order_id'}, inplace=True)
# Trasformation of DataFrame
first_customer_message.reset_index(inplace=True)
first_customer_message.rename(columns={'index': 'order_id'}, inplace=True)


# Number of messages sent by courier and customer
num_messages_courier = merged_df.loc[merged_df['sender_app_type'].str.startswith('Courier')].groupby('order_id').size()
num_messages_customer = merged_df.loc[merged_df['sender_app_type'].str.startswith('Customer')].groupby('order_id').size()


# Extracting who has sent the first message
first_message_by = merged_df.groupby('order_id')['sender_app_type'].min().apply(lambda x: 'courier' if x.startswith('Courier') else 'customer')
# Trasformation of DataFrame
first_message_by= first_message_by.to_frame().reset_index()

# Time first message
conversation_started_at = merged_df.groupby('order_id')['message_sent_time'].min()

# time last message
last_message_time = merged_df.groupby('order_id')['message_sent_time'].max()

# Order Stage of last message
last_message_order_stage = merged_df.loc[merged_df.groupby('order_id')['message_sent_time'].idxmax(), ['order_id', 'order_stage']]


# Computing difference of time
first_response_time = pd.merge(first_customer_message, first_courier_message, on='order_id', suffixes=('_customer', '_courier'))
first_response_time_delay_seconds = (first_response_time['message_sent_time_customer'] - first_response_time['message_sent_time_courier']).dt.total_seconds()
first_response_time['response_time']= first_response_time_delay_seconds

data = []
for order_id in merged_df['order_id'].drop_duplicates():

    row = {
        'order_id': order_id,
        'city_code': merged_df.loc[merged_df['order_id'] == order_id, 'city_code'].iloc[0],
        'first_courier_message': first_courier_message.loc[first_courier_message['order_id']==int(order_id)]['message_sent_time'].values,
        'first_customer_message': first_customer_message.loc[first_customer_message['order_id']==int(order_id)]['message_sent_time'].values,
        'num_messages_courier': num_messages_courier.get(order_id, None),
        'num_messages_customer': num_messages_customer.get(order_id, None),
        'first_message_by':first_message_by.loc[first_message_by['order_id']==int(order_id)]['sender_app_type'].values,
        'conversation_started_at': conversation_started_at.get(order_id, None),
        'first_response_time_delay_seconds': first_response_time.loc[first_response_time['order_id']== int(order_id)]['response_time'].values,
        'last_message_time': last_message_time.get(order_id, None),
        'last_message_order_stage': last_message_order_stage.loc[last_message_order_stage['order_id'] == order_id, 'order_stage'].iloc[0] if order_id in last_message_order_stage['order_id'].values else None
    }
    data.append(row)
result_df = pd.DataFrame(data)



result_df.to_excel('Output Files/Output.xlsx',index=False)