import sys
from pathlib import Path 
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

from app import * 
import warnings 
import pytest

warnings.filterwarnings('ignore')

payload = { 
"company_ids": "5cddce9a51cbb2002d636741",
"kiosk_ids": "5e5fc3e7b938260011e96747",
"product_ids":"5ce804504edb74002d2d16e9",
"start_date": "2023-12-25",
"end_date": "2023-12-25"
}

payload_1 = {
"company_ids": "5cddce9a51cbb2002d636741",
"kiosk_ids": "5e5fc3e7b938260011e96747,611a4f28660486114ff03e13",
"product_ids":"5ce804504edb74002d2d16e9,5ce8cf9ef5cc72002fee0d42,5ce8da44f5cc72002fee0d48,5ce8eb23388a910032be45d6, 5ceca8f17e36fc002fbfc98d,5cecc9ad20b264002fbbc308,5f1994b1c86e8175d4cf6e51,6094faa4d9a5ee25afffa033,623de896d3e0ad834d4dca50,623deb102e80e5092f53b517,5ce5be56a62928002d768a17,5d67d0a8616161002e0887b7,5daefe3e7ba10f002de2d0d8,5df0fd4f2f7328002f2f28b8,5ec38d1a96e18700112645ae,5f4f95684b0058bae6f6a04c,60924c23d9a5ee9976ff9b43,60b9f1c703d566b3b0d3f49a,60eea68eabe62947c749ad9b,5ce7f6e5fb52670030116ef4,5ce8da44f5cc72002fee0d48,5ce8e72bf5cc72002fee0d4b,5ce8eb23388a910032be45d6,5ce8ec9df5cc72002fee0d4e, 5cebfbc3fbd8d40030f37bb7,5d03dba8f5d1670030077f95,604f7069ee4217d1efd43f9a,60b9f1c703d566b3b0d3f49a,632d7f353946dce22950262c",
"start_date": "2024-05-01",
"end_date": "2024-05-12"
}

@pytest.fixture
def client():
    with app.test_client() as client:
        print('yielding client')
        yield client

def test_aggregated_revenue(client):
    response = client.post('/aggregated_revenue', json=payload)
    response_1 = client.post('/aggregated_revenue', json=payload_1)
    assert response.status_code == 200
    assert 'data' in response.json
    assert len(response_1.json['data']['list'])>0

def test_revenue_comparision(client):
    response = client.post('/revenue_comparision', json=payload)
    response_1 = client.post('/revenue_comparision', json=payload_1)
    assert response.status_code == 200
    assert 'list' in response.json
    assert len(response_1.json['list'])>0

def test_info_card(client):
    response = client.post('/generate_info_card', json=payload)
    response_1 = client.post('/generate_info_card', json=payload_1)
    assert response.status_code == 200
    assert 'forecastAccuracy' in response.json
    assert 'netSales' in response.json
    assert 'numberOfCustomer' in response.json
    assert 'productDemand' in response.json
    assert float(response_1.json['forecastAccuracy'])>0
    assert float(response_1.json['numberOfCustomer'])>0
    assert float(response_1.json['netSales'])>0
    assert float(response_1.json['productDemand'])>0

def test_aggregated_revenue_stats(client):
    response = client.post('/aggregated_revenue_stats', json=payload)
    response_1 = client.post('/aggregated_revenue_stats', json=payload_1)
    assert response.status_code == 200
    assert 'stat' in response.json
    assert int(response_1.json['stat']['lastWeekChangePercentage'])>0
    assert int(response_1.json['stat']['lastWeekChangeValue'])>0

def test_product_demand_forecast(client):
    response = client.post('/product_demand_forecast', json=payload)
    response_1 = client.post('/product_demand_forecast', json=payload_1)
    assert response.status_code == 200
    assert 'list' in response.json
    assert len(response_1.json['list'])>0

def test_product_demand_stats(client):
    response = client.post('/product_demand_forecast_stats', json=payload)
    response_1 = client.post('/product_demand_forecast_stats', json=payload_1)
    assert response.status_code == 200
    assert 'stat' in response.json
    assert int(response_1.json['stat']['lastWeekChangePercentage'])>0
    assert int(response_1.json['stat']['lastWeekChangeValue'])>0

def test_top_selling_products(client):
    response = client.post('/top_selling_products_table', json=payload)
    response_1 = client.post('/top_selling_products_table', json=payload_1)
    assert response.status_code == 200
    assert 'list' in response.json
    assert len(response_1.json['list'])>0
    assert response_1.json['list'][0]['quantitySold']>=response_1.json['list'][1]['quantitySold']

def test_top_selling_kiosks(client):
    response = client.post('/top_selling_kiosks_table', json=payload)
    response_1 = client.post('/top_selling_kiosks_table', json=payload_1)
    assert response.status_code == 200
    assert 'list' in response.json
    assert len(response_1.json['list'])>0
    assert response_1.json['list'][0]['netSale']>=response_1.json['list'][1]['netSale']

