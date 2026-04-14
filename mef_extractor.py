import urllib.request
import csv
import json
import io
import codecs
import ssl  
from datetime import datetime, timedelta
import time

def to_f(val):
    """Convierte strings con comas o vacíos a float de forma segura."""
    try:
        if not val:
            return 0.0
        return float(str(val).replace(',', '').strip())
    except (ValueError, TypeError):
        return 0.0

def generate_seguimiento_detallado():
    # URL de Gasto Diario 2026
    url = "https://fs.datosabiertos.mef.gob.pe/datastorefiles/2026-Gasto-Diario.csv"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # Configuración de Lambayeque
    CODIGO_PLIEGO_LAMBAYEQUE = "452"
    
    # Reintentos (Exponential Backoff)
    max_retries = 5
    retry_delay = 2 # Empezamos con un poco más de margen
    
    # Crear el contexto para ignorar el error de certificado vencido del MEF
    context = ssl._create_unverified_context()
    
    print(f"🚀 Iniciando extracción optimizada para Lambayeque (Pliego {CODIGO_PLIEGO_LAMBAYEQUE})...")

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            
            # Aumentamos el timeout a 1800 (30 min) por si el servidor del MEF está muy lento
            with urllib.request.urlopen(req, timeout=1800, context=context) as response:
                decoder = codecs.getreader("utf-8-sig")(response)
                reader = csv.DictReader(decoder)
                
                reader.fieldnames = [f.strip() for f in reader.fieldnames]
                
                proyectos_data = []
                count_total = 0
                count_filtered = 0

                print("📡 Conexión establecida. Procesando flujo de datos...")

                for r in reader:
                    count_total += 1
                    
                    # Filtro por Pliego
                    if r.get('PLIEGO') == CODIGO_PLIEGO_LAMBAYEQUE:
                        count_filtered += 1
                        
                        proyecto = {
                            "PRODUCTO_PROYECTO": r.get('PRODUCTO_PROYECTO', ''),
                            "PRODUCTO_PROYECTO_NOMBRE": r.get('PRODUCTO_PROYECTO_NOMBRE', 'SIN NOMBRE'),
                            "ANO_EJE": r.get('ANO_EJE', '2026'),
                            "EJECUTORA_NOMBRE": r.get('EJECUTORA_NOMBRE', 'SIN NOMBRE'),
                            "TIPO_ACT_PROY_NOMBRE": r.get('TIPO_ACT_PROY_NOMBRE', 'PROYECTO'),
                            "MONTO_PIA": to_f(r.get('MONTO_PIA')),
                            "MONTO_PIM": to_f(r.get('MONTO_PIM')),
                            "MONTO_CERTIFICADO": to_f(r.get('MONTO_CERTIFICADO')),
                            "MONTO_COMPROMETIDO": to_f(r.get('MONTO_COMPROMETIDO')),
                            "MONTO_COMPROMETIDO_ANUAL": to_f(r.get('MONTO_COMPROMETIDO_ANUAL')),
                            "MONTO_DEVENGADO": to_f(r.get('MONTO_DEVENGADO')),
                            "MONTO_GIRADO": to_f(r.get('MONTO_GIRADO'))
                        }
                        proyectos_data.append(proyecto)

                    # Feedback visual más frecuente (cada 50k líneas) para evitar cancelación de GitHub
                    if count_total % 50000 == 0:
                        print(f"⏳ Procesando... {count_total:,} líneas leídas | Encontrados: {count_filtered}")

                # Generar JSON final
                hora_peru = datetime.now() - timedelta(hours=5)
                objeto_final = {
                    "ultima_actualizacion": hora_peru.strftime("%d/%m/%Y %H:%M"),
                    "total_registros_lambayeque": count_filtered,
                    "proyectos": proyectos_data
                }

                file_name = 'data_gasto_lambayeque.json'
                with open(file_name, 'w', encoding='utf-8') as f:
                    json.dump(objeto_final, f, indent=2, ensure_ascii=False)
                
                print(f"✅ ¡Éxito! JSON generado: {file_name}")
                print(f"📊 Total Lambayeque: {count_filtered} registros de {count_total:,} procesados.")
                return 

        except Exception as e:
            print(f"⚠️ Intento {attempt + 1} fallido: {e}")
            if attempt < max_retries - 1:
                print(f"🔄 Reintentando en {retry_delay} segundos...")
                time.sleep(retry_delay)
                retry_delay *= 2 
            else:
                print("🚨 Se agotaron los reintentos. El servidor del MEF no responde correctamente.")

if __name__ == "__main__":
    generate_seguimiento_detallado()
