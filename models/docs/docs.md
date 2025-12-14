
{% docs fct_orders %}

# Fact Orders

The `fct_orders` model contains **order-level data** enriched with details about employees and companies, as well as a breakdown of costs.

## Logic behind Cost Calculations

### **Discounts**
Applies a **company loyalty discount** based on:
- How long the company has been a customer:
  - ≥ 5 years → 10%
  - ≥ 2 years → 5%
- Or if the company has placed more than 200 orders → 4%
- Otherwise → 0%

Applies an **employee loyalty discount** based on:
- When the employee has placed:
    - 100+ orders: 5%
    - Between 50 - 99 orders: 3%
    - Between 10 - 49 orders: 1%
- Otherwise → 0%

### **VAT Calculation**
Applies a **VAT varies per product category**:
- ≥ 5 years → 10%
- ≥ 2 years → 5%
- Or if the company has placed more than 200 orders → 4%
- Otherwise → 0%

{% enddocs %}
