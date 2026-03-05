import urllib.request, csv, json, io
from datetime import datetime, timedelta

def generate_seguimiento_detallado():
    # URL de Gasto Diario
    url = "https://fs.datosabiertos.mef.gob.pe/datastorefiles/2025-Gasto-Diario.csv"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # Código del Pliego Lambayeque
    CODIGO_PLIEGO_LAMBAYEQUE = "452"
    
    try:
        print(f"🚀 Extrayendo datos de Gasto Diario para Lambayeque...")
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=600) as response:
            content = response.read().decode('utf-8-sig')
            reader = csv.DictReader(io.StringIO(content))
            
            # Limpieza básica de nombres de columnas (solo quitar espacios)
            reader.fieldnames = [f.strip() for f in reader.fieldnames]
            
            proyectos_data = []

            for r in reader:
                # Filtro directo por Pliego
                if r.get('PLIEGO') == CODIGO_PLIEGO_LAMBAYEQUE:
                    
                    def to_f(val):
                        try:
                            return float(str(val).replace(',', '')) if val else 0.0
                        except:
                            return 0.0

                    # Estructura directa según tu diccionario
                    proyectos_data.append({
                        "PRODUCTO_PROYECTO": r.get('PRODUCTO_PROYECTO', ''),
                        "PRODUCTO_PROYECTO_NOMBRE": r.get('PRODUCTO_PROYECTO_NOMBRE', 'SIN NOMBRE'),
                        "ANO_EJE": r.get('ANO_EJE', '2026'),
                        "EJECUTORA_NOMBRE": r.get('EJECUTORA_NOMBRE', 'SIN NOMBRE'),
                        "TIPO_ACT_PROY_NOMBRE": r.get('TIPO_ACT_PROY_NOMBRE', 'PROYECTO'),
                        
                        # Campos de montos
                        "MONTO_PIA": to_f(r.get('MONTO_PIA')),
                        "MONTO_PIM": to_f(r.get('MONTO_PIM')),
                        "MONTO_CERTIFICADO": to_f(r.get('MONTO_CERTIFICADO')),
                        "MONTO_COMPROMETIDO": to_f(r.get('MONTO_COMPROMETIDO')),
                        "MONTO_COMPROMETIDO_ANUAL": to_f(r.get('MONTO_COMPROMETIDO_ANUAL')),
                        "MONTO_DEVENGADO": to_f(r.get('MONTO_DEVENGADO')),
                        "MONTO_GIRADO": to_f(r.get('MONTO_GIRADO'))
                    })

            # Generar JSON final
            hora_peru = datetime.now() - timedelta(hours=5)
            objeto_final = {
                "ultima_actualizacion": hora_peru.strftime("%d/%m/%Y %H:%M"),
                "proyectos": proyectos_data
            }

            with open('data_gasto_lambayeque_2025.json', 'w', encoding='utf-8') as f:
                json.dump(objeto_final, f, indent=2, ensure_ascii=False)
            
            print(f"✅ ¡Listo! JSON generado con {len(proyectos_data)} registros.")

    except Exception as e:
        print(f"🚨 Error: {e}")

if __name__ == "__main__":
    generate_seguimiento_detallado()
