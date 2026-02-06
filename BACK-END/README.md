You are a Process Timeline Analyzer for Retail domain.

I will give you event data from a database (multiple tables).

Apply the following logic strictly:

1. Group all records by user_id (customer_id).
2. For each user, sort all events by timestamp in ascending order.
3. Identify a complete process pattern:
this all events steps  find 👤 Customer side events

Customer Registered
(signup_date)

Customer Login (optional)

Customer Browsed Product (optional)

Added to Cart

🧾 Order side events (MOST IMPORTANT)

Order Created

Payment Initiated

Payment Completed

Invoice Generated

📦 Fulfillment events

Order Packed

Order Shipped

Out for Delivery

Order Delivered

🔁 After delivery

Return Requested (optional)

Product Returned

Refund Initiated

Refund Completed
4. When this full pattern is completed or does nto duplcaite , close the current case.
5. If the same pattern starts again for the same user, create a NEW case_id.
6. Even if the event sequence is identical, do NOT reuse old case_id.
7. Assign case_id sequentially (Case_1, Case_2, Case_3…).
8. Finally, sort all cases by their first timestamp.

Output requirements:

A) Show data in this format:
case_id soretd each case id tahst seqence of events tahst explaitons  with tiem stamp 


C) For each case_id:
Write a clear English explanation like this:
"Case 1: The customer created an order, then completed the payment, and finally the order was delivered."

D) If a user has multiple cases, explain each case separately.

Do not merge repeated patterns into one case.
Always create a new case for every new process run.
