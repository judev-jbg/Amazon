
import asyncio
import time
import pandas as pd
import logging
from datetime import datetime, timedelta
from sp_api.api import Orders, Sales
from sp_api.base import SellingApiException, Granularity
from sp_api.util import throttle_retry, load_all_pages
import config.setting as st

credentials = dict(
    refresh_token=st.setting_cred_api_amz['refresh_token'],
    lwa_app_id=st.setting_cred_api_amz['lwa_app_id'],  
    lwa_client_secret=st.setting_cred_api_amz['lwa_client_secret'],
    aws_secret_key=st.setting_cred_api_amz['aws_secret_key'],
    aws_access_key=st.setting_cred_api_amz['aws_access_key'], 
    role_arn=st.setting_cred_api_amz['role_arn']
)


def getOrder(orderId: str, tagSubjectMail: str = ''):
    """
    Recupera datos de una orden específica desde Amazon SP-API
    
    Args:
        orderId: ID de la orden de Amazon
        tagSubjectMail: Tag para identificación en logs
    
    Returns:
        [DataFrame, success_flag]: DataFrame con datos de la orden y flag de éxito
    """
    try:
        print("#" * 5, " ¡Proceso de recolección de orden específica! ", "#" * 5)   
        print("#" * 5, f" -Recuperando datos de la orden {orderId}")

        # Estructura para almacenar datos
        order_data = {
            'purchaseDate': [],
            'purchaseDateEs': [],
            'salesChannel': [],
            'amazonOrderId': [],
            'buyerEmail': [],
            'earliestShipDate': [],
            'latestShipDate': [],
            'earliestDeliveryDate': [],
            'latestDeliveryDate': [],
            'lastUpdateDate': [],
            'isBusinessOrder': [],
            'marketplaceId': [],
            'numberOfItemsShipped': [],
            'numberOfItemsUnshipped': [],
            'orderStatus': [],
            'totalOrderCurrencyCode': [],
            'totalOrderAmount': [],
            'city': [],
            'countryCode': [],
            'postalCode': [],
            'stateOrRegion': []
        }

        # Llamada a la API
        order_api = Orders(credentials=credentials).get_order(orderId)
        order = order_api.payload
        
        # Procesar datos de la orden
        order_data['purchaseDate'].append(order.get("PurchaseDate"))
        
        # Convertir fecha a timezone local
        str_time = order.get("PurchaseDate")
        str_time = str_time.replace('T', ' ').replace('Z', '')
        order_data['purchaseDateEs'].append(
            datetime.strptime(str_time, '%Y-%m-%d %H:%M:%S') + timedelta(hours=st.difHoursUtc)
        )
        
        # Datos básicos de la orden
        order_data['salesChannel'].append(order.get("SalesChannel"))
        order_data['amazonOrderId'].append(order.get("AmazonOrderId"))
        order_data['buyerEmail'].append(order.get("BuyerInfo", {}).get("BuyerEmail"))
        order_data['earliestShipDate'].append(order.get("EarliestShipDate"))
        order_data['latestShipDate'].append(order.get("LatestShipDate"))
        order_data['earliestDeliveryDate'].append(order.get("EarliestDeliveryDate"))
        order_data['latestDeliveryDate'].append(order.get("LatestDeliveryDate"))
        order_data['lastUpdateDate'].append(order.get("LastUpdateDate", "").replace('T', ' ').replace('Z', ''))
        order_data['isBusinessOrder'].append(order.get("IsBusinessOrder"))
        order_data['marketplaceId'].append(order.get("MarketplaceId"))
        order_data['numberOfItemsShipped'].append(order.get("NumberOfItemsShipped"))
        order_data['numberOfItemsUnshipped'].append(order.get("NumberOfItemsUnshipped"))
        order_data['orderStatus'].append(order.get("OrderStatus"))
        
        # Información financiera
        order_total = order.get("OrderTotal")
        if order_total:
            order_data['totalOrderCurrencyCode'].append(order_total.get("CurrencyCode", "S/D"))
            order_data['totalOrderAmount'].append(order_total.get("Amount", 0))
        else:
            order_data['totalOrderCurrencyCode'].append("S/D")
            order_data['totalOrderAmount'].append(0)
        
        # Dirección de envío
        shipping_address = order.get("ShippingAddress")
        if shipping_address:
            order_data['city'].append(shipping_address.get("City", "S/D"))
            order_data['countryCode'].append(shipping_address.get("CountryCode", "S/D"))
            order_data['postalCode'].append(shipping_address.get("PostalCode", "S/D"))
            order_data['stateOrRegion'].append(shipping_address.get("StateOrRegion", "S/D"))
        else:
            order_data['city'].append("S/D")
            order_data['countryCode'].append("S/D")
            order_data['postalCode'].append("S/D")
            order_data['stateOrRegion'].append("S/D")

        # Crear DataFrame
        df_order = pd.DataFrame(order_data)
        
        # Añadir columnas con sufijo para diferenciación
        df_order.columns = [col + '_o' for col in df_order.columns]
        
        if len(df_order.index) > 0:
            print("#" * 5, " -El proceso de recolección de orden finalizó con éxito")
            print("#" * 5, "-" * 70)
            return [df_order, 1]
        else:
            print("#" * 5, " -El proceso de recolección de orden finalizó pero no obtuvo resultados")
            print("#" * 5, "-" * 70)
            return [pd.DataFrame(), 1]

    except SellingApiException as ex:
        error_context = {
            'function': 'getOrder',
            'orderId': orderId,
            'error_type': 'SellingApiException',
            'api_code': getattr(ex, 'code', None)
        }
        
        # Si es rate limit, retornar código de error para retry
        if hasattr(ex, 'code') and ex.code == 429:
            print("#" * 5, f" -Rate limit alcanzado para orden {orderId}")
            return [pd.DataFrame([ex.code], columns=["code"]), 0]
        
        # Para otros errores de API, usar el sistema de manejo de errores
        from infrastructure.error_handling import EnhancedErrorHandler
        error_handler = EnhancedErrorHandler()
        asyncio.create_task(error_handler.handle_error(ex, error_context))
        
        return [pd.DataFrame(), 0]

    except Exception as ex:
        error_context = {
            'function': 'getOrder',
            'orderId': orderId,
            'tagSubjectMail': tagSubjectMail
        }
        
        # Usar el sistema de manejo de errores
        from infrastructure.error_handling import EnhancedErrorHandler
        error_handler = EnhancedErrorHandler()
        asyncio.create_task(error_handler.handle_error(ex, error_context))
        
        return [pd.DataFrame(), 0]

def getOrders(dateInit: str, dateEnd: str, market: list, context: dict = None) -> tuple:
    """
    Extrae órdenes de Amazon SP-API
    
    Args:
        dateInit: Fecha inicio en formato ISO
        dateEnd: Fecha fin en formato ISO  
        market: Lista de mercados
        context: Contexto para manejo de errores
        
    Returns:
        tuple: (DataFrame con órdenes, success_flag)
    
    """
    logger = logging.getLogger(f"{__name__}.getOrders")
    
    try:
        logger.debug(f"Extracting orders from {dateInit} to {dateEnd} for markets: {market}")
        
        # Inicializar listas para datos
        purchaseDateAMZ = []
        purchaseDateEs = []
        salesChannel = []
        amazonOrderId = []
        buyerEmail = []
        earliestShipDate = []
        latestShipDate = []
        earliestDeliveryDate = []
        latestDeliveryDate = []
        lastUpdateDate = []
        isBusinessOrder = []
        marketplaceId = []
        numberOfItemsShipped = []
        numberOfItemsUnshipped = []
        orderStatus = []
        currencyCode = []
        amount = []
        city = []
        countryCode = []
        postalCode = []
        stateOrRegion = []

        @throttle_retry()
        @load_all_pages()
        def load_all_orders(**kwargs):
            """Función interna para paginación automática"""
            return Orders(credentials=credentials).get_orders(**kwargs)
        
        # Extraer órdenes con paginación automática
        for page in load_all_orders(CreatedAfter=dateInit, CreatedBefore=dateEnd, MarketplaceIds=market):
            for order in page.payload.get("Orders", []):
                # Transformar datos
                purchaseDateAMZ.append(order.get("PurchaseDate"))
                
                # Convertir fecha a timezone local
                str_time = order.get("PurchaseDate", "")
                str_time = str_time.replace('T', ' ').replace('Z', '')
                if str_time:
                    local_date = datetime.strptime(str_time, '%Y-%m-%d %H:%M:%S') + timedelta(hours=st.difHoursUtc)
                    purchaseDateEs.append(local_date)
                else:
                    purchaseDateEs.append(None)
                
                # Extraer campos básicos
                salesChannel.append(order.get("SalesChannel"))
                amazonOrderId.append(order.get("AmazonOrderId"))
                buyerEmail.append(order.get("BuyerInfo", {}).get("BuyerEmail"))
                earliestShipDate.append(order.get("EarliestShipDate"))
                latestShipDate.append(order.get("LatestShipDate"))
                earliestDeliveryDate.append(order.get("EarliestDeliveryDate"))
                latestDeliveryDate.append(order.get("LatestDeliveryDate"))
                
                # Limpiar lastUpdateDate
                last_update = order.get("LastUpdateDate", "")
                lastUpdateDate.append(last_update.replace('T', ' ').replace('Z', '') if last_update else None)
                
                isBusinessOrder.append(order.get("IsBusinessOrder", False))
                marketplaceId.append(order.get("MarketplaceId"))
                numberOfItemsShipped.append(order.get("NumberOfItemsShipped", 0))
                numberOfItemsUnshipped.append(order.get("NumberOfItemsUnshipped", 0))
                orderStatus.append(order.get("OrderStatus"))
                
                # Manejar OrderTotal (puede ser None)
                order_total = order.get("OrderTotal")
                if order_total:
                    currencyCode.append(order_total.get("CurrencyCode", "S/D"))
                    amount.append(order_total.get("Amount", 0))
                else:
                    currencyCode.append("S/D")
                    amount.append(0)
                
                # Manejar ShippingAddress (puede ser None)
                shipping_addr = order.get("ShippingAddress")
                if shipping_addr:
                    city.append(shipping_addr.get("City", "S/D"))
                    countryCode.append(shipping_addr.get("CountryCode", "S/D"))
                    postalCode.append(shipping_addr.get("PostalCode", "S/D"))
                    stateOrRegion.append(shipping_addr.get("StateOrRegion", "S/D"))
                else:
                    city.append("S/D")
                    countryCode.append("S/D")
                    postalCode.append("S/D")
                    stateOrRegion.append("S/D")
            
            # Small delay entre páginas para ser gentil con la API
            time.sleep(1)

        # Crear DataFrame
        df_orders = pd.DataFrame({
            'purchaseDate': purchaseDateAMZ,
            'purchaseDateEs': purchaseDateEs,
            'salesChannel': salesChannel,
            'amazonOrderId': amazonOrderId,
            'buyerEmail': buyerEmail,
            'earliestShipDate': earliestShipDate,
            'latestShipDate': latestShipDate,
            'earliestDeliveryDate': earliestDeliveryDate,
            'latestDeliveryDate': latestDeliveryDate,
            'lastUpdateDate': lastUpdateDate,
            'isBusinessOrder': isBusinessOrder,
            'marketplaceId': marketplaceId,
            'numberOfItemsShipped': numberOfItemsShipped,
            'numberOfItemsUnshipped': numberOfItemsUnshipped,
            'orderStatus': orderStatus,
            'totalOrderCurrencyCode': currencyCode,
            'totalOrderAmount': amount,
            'city': city,
            'countryCode': countryCode,
            'postalCode': postalCode,
            'stateOrRegion': stateOrRegion
        })
        
        # Añadir metadatos
        df_orders['loadDate'] = datetime.now().date()
        df_orders['loadDateTime'] = datetime.now()
        
        logger.info(f"Successfully extracted {len(df_orders)} orders")
        return df_orders, True
        
    except SellingApiException as e:
        # Dejar que la infraestructura de errores maneje esto
        logger.error(f"Amazon API error in getOrders: {e}")
        error_context = {
            'function': 'getOrders',
            'error_type': 'SellingApiException',
            'api_code': getattr(e, 'code', None)
        }
        
        # Si es rate limit, retornar código de error para retry
        if hasattr(e, 'code') and e.code == 429:
            print("#" * 5, f" -Rate limit alcanzado para ordenes")
            return [pd.DataFrame([e.code], columns=["code"]), 0]
        
        # Para otros errores de API, usar el sistema de manejo de errores
        from infrastructure.error_handling import EnhancedErrorHandler
        error_handler = EnhancedErrorHandler()
        asyncio.create_task(error_handler.handle_error(e, error_context))
        
        return [pd.DataFrame(), 0]

        
    except Exception as e:
        # Dejar que la infraestructura de errores maneje esto
        logger.error(f"Unexpected error in getOrders: {e}")
        error_context = {
            'function': 'getOrders'
        }
        
        # Usar el sistema de manejo de errores
        from infrastructure.error_handling import EnhancedErrorHandler
        error_handler = EnhancedErrorHandler()
        asyncio.create_task(error_handler.handle_error(e, error_context))
        
        return [pd.DataFrame(), 0]

def getOrderItems(orderId: str, tagSubjectMail: str = ''):
    """
    Recupera elementos de una orden específica desde Amazon SP-API
    
    Args:
        orderId: ID de la orden de Amazon
        tagSubjectMail: Tag para identificación en logs
    
    Returns:
        [DataFrame, success_flag]: DataFrame con elementos de la orden y flag de éxito
    """
    try:
        print("#" * 5, " ¡Proceso de recolección de elementos de orden! ", "#" * 5)   
        print("#" * 5, f" -Recuperando elementos de la orden {orderId}")

        # Estructura para almacenar datos
        order_items_data = {
            'orderId': [],
            'orderItemId': [],
            'asin': [],
            'sku': [],
            'title': [],
            'conditionItem': [],
            'nItems': [],
            'qOrdered': [],
            'qShipped': [],
            'reasonCancel': [],
            'isRequestedCancel': [],
            'itemPriceCurrencyCode': [],
            'itemPriceCurrencyAmount': [],
            'itemTaxCurrencyCode': [],
            'itemTaxCurrencyAmount': []
        }

        # Llamada a la API
        order_items = Orders(credentials=credentials).get_order_items(orderId)

        # Procesar cada elemento de la orden
        for item in order_items.payload.get("OrderItems", []):
            order_items_data['orderId'].append(orderId)
            order_items_data['orderItemId'].append(item.get("OrderItemId"))
            order_items_data['asin'].append(item.get("ASIN"))
            order_items_data['sku'].append(item.get("SellerSKU"))
            order_items_data['title'].append(item.get("Title"))
            order_items_data['conditionItem'].append(item.get("ConditionId"))
            
            # Información del producto
            product_info = item.get("ProductInfo", {})
            order_items_data['nItems'].append(product_info.get("NumberOfItems"))
            
            # Cantidades
            order_items_data['qOrdered'].append(item.get("QuantityOrdered"))
            order_items_data['qShipped'].append(item.get("QuantityShipped"))
            
            # Información de cancelación
            buyer_cancel = item.get("BuyerRequestedCancel")
            if buyer_cancel:
                order_items_data['reasonCancel'].append(buyer_cancel.get("BuyerCancelReason", "S/D"))
                order_items_data['isRequestedCancel'].append(buyer_cancel.get("IsBuyerRequestedCancel", 0))
            else:
                order_items_data['reasonCancel'].append("S/D")
                order_items_data['isRequestedCancel'].append(0)
            
            # Información de precios
            item_price = item.get("ItemPrice")
            if item_price:
                order_items_data['itemPriceCurrencyCode'].append(item_price.get("CurrencyCode", "S/D"))
                order_items_data['itemPriceCurrencyAmount'].append(item_price.get("Amount", 0))
            else:
                order_items_data['itemPriceCurrencyCode'].append("S/D")
                order_items_data['itemPriceCurrencyAmount'].append(0)
            
            # Información de impuestos
            item_tax = item.get("ItemTax")
            if item_tax:
                order_items_data['itemTaxCurrencyCode'].append(item_tax.get("CurrencyCode", "S/D"))
                order_items_data['itemTaxCurrencyAmount'].append(item_tax.get("Amount", 0))
            else:
                order_items_data['itemTaxCurrencyCode'].append("S/D")
                order_items_data['itemTaxCurrencyAmount'].append(0)

        # Crear DataFrame
        df_order_items = pd.DataFrame(order_items_data)
        df_order_items['loadDate'] = str(datetime.date(datetime.now()))
        df_order_items['loadDateTime'] = datetime.now()

        if len(df_order_items.index) > 0:
            print("#" * 5, " -El proceso de recolección de elementos finalizó con éxito")
            print("#" * 5, "-" * 70)
            return [df_order_items, 1]
        else:
            print("#" * 5, " -El proceso de recolección de elementos finalizó pero no obtuvo resultados")
            print("#" * 5, "-" * 70)
            return [pd.DataFrame(), 1]

    except SellingApiException as ex:
        error_context = {
            'function': 'getOrderItems',
            'orderId': orderId,
            'error_type': 'SellingApiException',
            'api_code': getattr(ex, 'code', None)
        }
        
        # Si es rate limit, retornar código de error para retry
        if hasattr(ex, 'code') and ex.code == 429:
            print("#" * 5, f" -Rate limit alcanzado para elementos de orden {orderId}")
            return [pd.DataFrame([ex.code], columns=["code"]), 0]
        
        # Para otros errores de API, usar el sistema de manejo de errores
        from infrastructure.error_handling import EnhancedErrorHandler
        error_handler = EnhancedErrorHandler()
        asyncio.create_task(error_handler.handle_error(ex, error_context))
        
        return [pd.DataFrame(), 0]

    except Exception as ex:
        error_context = {
            'function': 'getOrderItems',
            'orderId': orderId,
            'tagSubjectMail': tagSubjectMail
        }
        
        # Usar el sistema de manejo de errores
        from infrastructure.error_handling import EnhancedErrorHandler
        error_handler = EnhancedErrorHandler()
        asyncio.create_task(error_handler.handle_error(ex, error_context))
        
        return [pd.DataFrame(), 0]

def getSales(asinp: str, skup: str, market: list, intervalp: tuple, tagSubjectMail: str = ''):
    """
    Recupera métricas de ventas para un ASIN/SKU específico desde Amazon SP-API
    
    Args:
        asinp: ASIN del producto
        skup: SKU del producto  
        market: Lista de mercados
        intervalp: Tupla con intervalo de fechas
        tagSubjectMail: Tag para identificación en logs
    
    Returns:
        [DataFrame, success_flag]: DataFrame con métricas de ventas y flag de éxito
    """
    mkt = getNameMarket(market[0])
    
    try:
        print("#" * 5, " ¡Proceso de recolección de métricas de ventas! ", "#" * 5)   
        print("#" * 5, f" -Recuperando métricas para ASIN {asinp} del mercado {mkt[0]}")

        # Estructura para almacenar datos
        sales_data = {
            'asin': [],
            'sku': [],
            'marketplaceId': [],
            'intervalHour': [],
            'qOrders': [],
            'avgPriceUndCurrencyCode': [],
            'avgPriceUndAmount': [],
            'undSold': [],
            'totalPriceSoldCurrencyCode': [],
            'totalPriceSoldAmount': []
        }

        # Llamada a la API - usando granularidad por hora
        sales = Sales(credentials=credentials).get_order_metrics(
            interval=intervalp, 
            granularity=Granularity.HOUR, 
            asin=asinp, 
            marketplaceIds=market
        )

        # Procesar cada métrica de venta
        for item_sale in sales.payload:
            # Solo procesar si hay ventas
            if item_sale.get('unitCount', 0) > 0:
                sales_data['asin'].append(asinp)
                sales_data['sku'].append(skup)
                sales_data['marketplaceId'].append(market[0])
                sales_data['intervalHour'].append(item_sale.get('interval'))
                sales_data['qOrders'].append(item_sale.get('orderCount'))
                sales_data['undSold'].append(item_sale.get('unitCount'))
                
                # Precio promedio por unidad
                avg_price = item_sale.get('averageUnitPrice', {})
                sales_data['avgPriceUndCurrencyCode'].append(avg_price.get('currencyCode'))
                sales_data['avgPriceUndAmount'].append(avg_price.get('amount'))
                
                # Total de ventas
                total_sales = item_sale.get('totalSales', {})
                sales_data['totalPriceSoldCurrencyCode'].append(total_sales.get('currencyCode'))
                sales_data['totalPriceSoldAmount'].append(total_sales.get('amount'))

        # Crear DataFrame solo si hay datos
        if any(sales_data.values()):
            df_sales = pd.DataFrame(sales_data)
            
            # Procesar campos de fecha/hora
            df_sales['auxDate'] = df_sales['intervalHour']
            df_sales['saleDate'] = df_sales['intervalHour'].str.slice(stop=10)
            df_sales['intervalHour'] = df_sales['intervalHour'].str.slice(start=11, stop=16)
            
            # Fecha/hora completa
            df_sales['saleDateTime'] = df_sales['auxDate'].str.slice(stop=16)
            df_sales['saleDateTime'] = df_sales['saleDateTime'].str.replace("T", " ", regex=True)
            
            # Convertir a timezone local (España)
            df_sales['saleDateEs'] = df_sales['auxDate'].str.slice(stop=16)
            df_sales['saleDateEs'] = df_sales['saleDateEs'].str.replace("T", " ", regex=True)
            df_sales['saleDateEs'] = pd.to_datetime(df_sales['saleDateEs'], format='%Y-%m-%d %H:%M') + timedelta(hours=2)
            df_sales['auxDate'] = df_sales['saleDateEs'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df_sales['saleDateEs'] = df_sales['auxDate'].str.slice(stop=10)
            df_sales['intervalHourEs'] = df_sales['auxDate'].str.slice(start=11, stop=16)
            
            # Campos de auditoría
            df_sales['loadDate'] = str(datetime.date(datetime.now()))
            df_sales['loadDateTime'] = datetime.now()
            df_sales = df_sales.drop(columns=["auxDate"])

            print("#" * 5, " -El proceso de recolección de métricas de ventas se realizó con éxito")
            print("#" * 5, "-" * 70)
            return [df_sales, 1]
        else:
            print("#" * 5, " -El proceso de recolección de métricas de ventas finalizó pero no obtuvo resultados")
            print("#" * 5, "-" * 70)
            return [pd.DataFrame(), 1]

    except SellingApiException as ex:
        error_context = {
            'function': 'getSales',
            'asinp': asinp,
            'skup': skup,
            'market': market[0] if market else None,
            'error_type': 'SellingApiException',
            'api_code': getattr(ex, 'code', None)
        }
        
        # Si es rate limit, retornar código de error para retry
        if hasattr(ex, 'code') and ex.code == 429:
            print("#" * 5, f" -Rate limit alcanzado para métricas de ASIN {asinp} en mercado {mkt[0]}")
            return [pd.DataFrame([ex.code], columns=["code"]), 0]
        
        # Para otros errores de API, usar el sistema de manejo de errores
        from infrastructure.error_handling import EnhancedErrorHandler
        error_handler = EnhancedErrorHandler()
        asyncio.create_task(error_handler.handle_error(ex, error_context))
        
        return [pd.DataFrame(), 0]

    except Exception as ex:
        error_context = {
            'function': 'getSales',
            'asinp': asinp,
            'skup': skup,
            'market': market[0] if market else None,
            'tagSubjectMail': tagSubjectMail
        }
        
        # Usar el sistema de manejo de errores
        from infrastructure.error_handling import EnhancedErrorHandler
        error_handler = EnhancedErrorHandler()
        asyncio.create_task(error_handler.handle_error(ex, error_context))
        
        return [pd.DataFrame(), 0]

def getNameMarket(idMarket: str):
    market = {
        "A1RKKUPIHCS9HS":["España","https://www.amazon.es/sp?ie=UTF8&seller="],
        "A1PA6795UKMFR9":["Alemania","https://www.amazon.de/sp?ie=UTF8&seller="],
        "APJ6JRA9NG5V4":["Italia","https://www.amazon.it/sp?ie=UTF8&seller="],
        "A1805IZSGTT6HS":["Paises bajos","https://www.amazon.nl/sp?ie=UTF8&seller="],
        "AMEN7PMS3EDWL":["Belgica","https://www.amazon.be/sp?ie=UTF8&seller="]
    }
    return market[idMarket]