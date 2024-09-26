from flask import Flask, jsonify, request
from sqlalchemy import create_engine, select, func, MetaData, Table
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from sqlalchemy.sql import and_, or_

app = Flask(__name__)

DATABASE_URL = "postgresql://avya:mypassword@localhost/motorq"
engine = create_engine(DATABASE_URL)
metadata = MetaData()
metadata.reflect(bind=engine)

vehicles = metadata.tables['vehicles']
trips = metadata.tables['trips']
owners = metadata.tables['owners']
sensors = metadata.tables['sensors']
maintenance = metadata.tables['maintenance']

@app.route('/total_distance_last_30_days', methods=['GET'])
def get_total_distance_last_30_days():
    Session = sessionmaker(bind=engine)
    session = Session()
    
    today = datetime.now()
    thirty_days_ago = today - timedelta(days=30)

    query = (
        select(
            vehicles.c.make,
            vehicles.c.model,
            owners.c.name.label("owner_name"),
            func.sum(trips.c.distance_traveled).label("total_distance")
        )
        .select_from(
            vehicles
            .join(owners, vehicles.c.owner_id == owners.c.owner_id)
            .join(trips, vehicles.c.vehicle_id == trips.c.vehicle_id)
        )
        .where(and_(trips.c.start_time >= thirty_days_ago, trips.c.start_time <= today))
        .group_by(vehicles.c.make, vehicles.c.model, owners.c.name)
    )

    results = session.execute(query).fetchall()
    session.close()

    distances = [
        {"make": result.make, "model": result.model, "owner_name": result.owner_name, "total_distance": float(result.total_distance)}
        for result in results
    ]

    return jsonify(distances)

@app.route('/sensor_anomalies', methods=['GET'])
def find_sensor_anomalies():
    Session = sessionmaker(bind=engine)
    session = Session()

    anomaly_query = select(
        vehicles.c.make,
        vehicles.c.model,
        sensors.c.sensor_type,
        sensors.c.sensor_reading,
        sensors.c.timestamp
    ).select_from(
        sensors.join(vehicles, sensors.c.vehicle_id == vehicles.c.vehicle_id)
    ).where(
        or_(
            and_(sensors.c.sensor_type == "Speed", sensors.c.sensor_reading > 120),
            and_(sensors.c.sensor_type == "Fuel Level", sensors.c.sensor_reading < 10)
        )
    )

    results = session.execute(anomaly_query).fetchall()
    session.close()

    anomalies = [
        {"make": result.make, "model": result.model, "sensor_type": result.sensor_type, "value": float(result.sensor_reading), "timestamp": result.timestamp}
        for result in results
    ]

    return jsonify(anomalies)

@app.route('/maintenance_history/<int:vehicle_id>', methods=['GET'])
def get_maintenance_history(vehicle_id):
    Session = sessionmaker(bind=engine)
    session = Session()

    maintenance_query = select(
        maintenance.c.maintenance_type,
        maintenance.c.maintenance_date,
        maintenance.c.maintenance_cost
    ).where(
        maintenance.c.vehicle_id == vehicle_id
    )

    results = session.execute(maintenance_query).fetchall()
    session.close()

    maintenance_history = [
        {"maintenance_type": result.maintenance_type, "maintenance_date": result.maintenance_date, "maintenance_cost": float(result.maintenance_cost)}
        for result in results
    ]

    return jsonify(maintenance_history)

@app.route('/frequent_trippers', methods=['GET'])
def find_frequent_trippers():
    Session = sessionmaker(bind=engine)
    session = Session()

    seven_days_ago = datetime.now() - timedelta(days=7)

    frequent_trippers_query = (
        select(
            vehicles.c.vehicle_id,
            vehicles.c.make,
            vehicles.c.model,
            func.count(trips.c.trip_id).label('num_trips')
        )
        .join(trips, vehicles.c.vehicle_id == trips.c.vehicle_id)
        .where(trips.c.start_time >= seven_days_ago)
        .group_by(vehicles.c.vehicle_id)
        .having(func.count(trips.c.trip_id) > 5)
    )

    results = session.execute(frequent_trippers_query).fetchall()
    session.close()

    frequent_trippers = [
        {"vehicle_id": result.vehicle_id, "make": result.make, "model": result.model, "num_trips": result.num_trips}
        for result in results
    ]

    return jsonify(frequent_trippers)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
