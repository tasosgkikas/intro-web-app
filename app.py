
import settings
import sys,os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
from lib import pymysql


def connection():
    ''' Use this function to create your connections '''
    return pymysql.connect(
        settings.mysql_host, 
        settings.mysql_user, 
        settings.mysql_passwd, 
        settings.mysql_schema)


def wrong_input() -> list[tuple]:
    return [('Wrong input!',)]


def table_with_column_names(dicts_list: list[dict]) -> list[tuple]:
    if not dicts_list:
        raise ValueError("Empty list as argument. Expected a non-empty list.")
    
    # The first row of the returned table should contain the column names,
    # which are the keys of any dict, for example the 1st dict (at index 0). 
    attributes: list[tuple] = [tuple(dicts_list[0].keys())]

    # The rest of the computed data are put into a list of (many) tuples,
    values: list[tuple] = [tuple(each_dict.values()) for each_dict in dicts_list]

    return attributes + values
    

def  findTrips(x,a,b):
    # Creating a DictCursor on a new connection.
    cursor = connection().cursor(cursor = pymysql.cursors.DictCursor)

    
    # Finding trip_ids and number of reservations for each trip_id.
    sql_trip_ids_rsvs = f"""
        select      
            tp.trip_package_id as trip_id, 
            count(distinct r.Reservation_id) as reservations
        from        
            trip_package tp, 
            reservation r
        where       
            r.offer_trip_package_id = tp.trip_package_id and
            r.travel_agency_branch_id = {x} and
            tp.trip_start >= "{a}" and tp.trip_start <= "{b}"
        group by
            tp.trip_package_id;
        """
    try:
        cursor.execute(sql_trip_ids_rsvs)
    except (
        pymysql.err.OperationalError, 
        pymysql.err.ProgrammingError
    ) as error:
        print(repr(error))
        return wrong_input()
    
    # The DictCursor returns the results as a list[dict], instead of list[tuple].
    # The keys of each dict are the column names of the "returned" table.
    trip_dicts: list[dict] = cursor.fetchall()
    if not trip_dicts: 
        return [("No results",)]
    

    # Finding the drivers that correspond to the given branch x.
    # The drivers depend only on the branch, not on the trip.
    # Therefore, all trips get the same drivers.
    sql_drivers = f"""
    select 
        e.name, 
        e.surname
    from 
        employees e, 
        drivers d 
    where 
        d.driver_employee_AM = e.employees_AM and 
        e.travel_agency_branch_travel_agency_branch_id = {x}
    """
    cursor.execute(sql_drivers)
    # There might be >1 drivers employeed at branch x.
    driver_dicts: list[dict[str, str]] = cursor.fetchall()

    # keeping only the values of the dicts as (name, surname) tuples of strings.
    driver_tuples: list[tuple[str]] = [
        tuple(driver_dict.values()) for driver_dict in driver_dicts 
    ]

    # The strings of each (name, surname) tuple are joined with a space, 
    # to create a string representing the full-name of each driver.
    driver_strings: list[str] = [
        " ".join(driver_tuple) for driver_tuple in driver_tuples
    ]

    # The full-names of all drivers are joined with commas,
    # to create a single string with all names.
    drivers_string: str = ", ".join(driver_strings)

    # Inside the "for" block, each trip_dict will be updated with the following dict.
    # It is computed and stored now so as not to be computed at every iteration.
    drivers_dict = {"branch_drivers": drivers_string if drivers_string else "No drivers"}
    

    # The rest of the queried data are computed individually for each trip_id.
    for trip_dict in trip_dicts:
        trip_dict.update(drivers_dict)


        # Finding some of the rest queried data and updating the trip_dict
        sql_data = f"""
        select 
            tp.cost_per_person, 
            tp.max_num_participants, 
            tp.trip_start, 
            tp.trip_end
        from 
            trip_package tp
        where 
            tp.trip_package_id = {trip_dict["trip_id"]}
        """
        cursor.execute(sql_data)
        data: dict = cursor.fetchone()  # there's only one row
        if data:
            trip_dict.update(data)
            trip_dict["empty_seats"]: int = \
                trip_dict["max_num_participants"] - trip_dict["reservations"]


        # Finding travel guides and updating the trip_dict
        sql_guide = f"""
        select 
            distinct 
            e.name, 
            e.surname
        from 
            guided_tour gt, 
            employees e
        where 
            e.employees_AM = gt.travel_guide_employee_AM and
            gt.trip_package_id = {trip_dict["trip_id"]};
        """
        cursor.execute(sql_guide)
        # Same logic as for the drivers, but much more compressed
        guides_str = ", ".join([
            " ".join(guide_tuple) for guide_tuple in [
                tuple(guide_dict.values()) for guide_dict in 
                    cursor.fetchall()  # >=1 travel guides for this trip
            ]
        ])
        trip_dict["guides"] = guides_str if guides_str else "No guides"

        # The 'trip_id' key-value pairs are not requested to be returned
        # and are not needed anymore. Therefore, they are excluded from the result dicts
        trip_dict.pop("trip_id")

    return table_with_column_names(trip_dicts)


def findRevenue(x):
    # Creating a DictCursor on a new connection.
    cursor = connection().cursor(cursor = pymysql.cursors.DictCursor)

    if x not in ['asc', 'desc', 'ASC', 'DESC', '', ' ']:
        return wrong_input()


    # Income and num of reservations PER BRANCH.
    sql_income = f"""
    select		
        r.travel_agency_branch_id as branch_id,
        count(*) as reservations,
        sum(o.cost) as income
    from reservation r, offer o
    where r.offer_id = o.offer_id
    group by branch_id
    order by income {x};
    """
    cursor.execute(sql_income)
    branch_dicts: list[dict] = cursor.fetchall()


    # Num of employees and total salaries PER BRANCH.
    sql_employees = f"""
    select
        travel_agency_branch_travel_agency_branch_id as branch_id,
        count(employees_AM) as number_of_employees,
        sum(salary) as total_salaries
    from employees
    group by branch_id;
    """
    cursor.execute(sql_employees)
    employees_dicts: list[dict] = cursor.fetchall()
    # Each one of these dicts corresponds to one branch_id and vice-versa.
    # To avoid nested for-loops of O(n^2) complexity, we convert this list
    # of dicts to a dict with the branch_ids as keys and the corresponding
    # dicts from the list as values. This way, linear search of O(n) time
    # complexity is not needed because we now have hash-table-like access 
    # to each dict with O(1) time complexity.
    employees_dicts: dict[int, dict] = {
        employees_dict['branch_id']: employees_dict
        for employees_dict in employees_dicts
    }

    for branch_dict in branch_dicts:
        branch_id: int = branch_dict['branch_id']
        employees_dict = employees_dicts[branch_id]  # this is where we avoid nested for
        branch_dict.update(employees_dict)
    
    return table_with_column_names(branch_dicts)


def bestClient(x):
    # Creating a DictCursor on a new connection.
    cursor = connection().cursor(cursor = pymysql.cursors.DictCursor)


    sql_best_clients = f"""
    select
        t.traveler_id as client_id, 
        t.name, t.surname, 
        sum(o.cost) as revenue
    from
        traveler t, 
        reservation r,
        offer o
    where
        t.traveler_id = r.Customer_id and
        r.offer_id = o.offer_id
    group by t.traveler_id, t.name, t.surname 
    order by revenue desc;
    """
    cursor.execute(sql_best_clients)
    client_dicts: list[dict] = cursor.fetchall()
    best_client_dicts: list[dict] = []


    for client_dict in client_dicts:
        # Clients are ordered by descending revenue =>
        # The best client is certainly the first one =>
        # If the current client has less revenue than the best,
        # then all best clients are already found =>
        # The rest need not be accounted for.
        if client_dict['revenue'] < client_dicts[0]['revenue']: break
        
        best_client_dicts.append(client_dict)
    

        sql_destinations = f"""
        select 
            count(distinct d.name) as cities, 
            count(distinct d.country) as countries
        from 
            reservation r,
            trip_package_has_destination tphd,
            destination d
        where 
            d.destination_id = tphd.destination_destination_id and 
            tphd.trip_package_trip_package_id = r.offer_trip_package_id 
        group by r.Customer_id 
        having r.Customer_id = {client_dict["client_id"]}
        """
        cursor.execute(sql_destinations)
        data_dict = cursor.fetchone()
        if data_dict: client_dict.update(data_dict)


        sql_attractions = f"""
        select ta.name as attraction
        from tourist_attraction ta, reservation r, guided_tour gt 
        where 
            ta.tourist_attraction_id = gt.tourist_attraction_id and 
            gt.trip_package_id = r.offer_trip_package_id and 
            r.Customer_id = {client_dict["client_id"]};
        """
        cursor.execute(sql_attractions)
        attraction_dicts = cursor.fetchall()

        if attraction_dicts: client_dict['attractions'] = ', '.join([
            attraction_dict['attraction'] for attraction_dict in attraction_dicts
        ])


        client_dict.pop("client_id")

    return table_with_column_names(best_client_dicts)
    

def giveAway(N):
    # Creating a DictCursor on a new connection.
    db = connection()
    cursor = db.cursor(cursor = pymysql.cursors.DictCursor)

    N = int(N)  # for future computations
    if N <= 0: return wrong_input()


    # Finding the number of trips that have at least one destination
    sql_trips_with_destinations = """
    select count(distinct trip_package_id) as trips_num
    from trip_package
    where exists (
        select destination_destination_id
        from trip_package_has_destination
        where trip_package_trip_package_id = trip_package_id
    )
    """
    cursor.execute(sql_trips_with_destinations)
    max_trips = cursor.fetchone()['trips_num']
    # Limiting Ν to this number
    if N > max_trips: N = max_trips


    sql_traveler_ids = "select traveler_id from traveler"
    cursor.execute(sql_traveler_ids)
    traveler_ids: list[int] = [
        traveler_dict['traveler_id']
        for traveler_dict in cursor.fetchall()
    ]

    max_customers = len(traveler_ids)
    # Limiting N to the maximum number of customers.
    if N > max_customers: N = max_customers

    # Choosing N different travelers
    import random
    lucky_traveler_ids: list[int] = random.sample(traveler_ids, N)


    result_messages: list[tuple[str]] = []
    gift_trip_ids: list[int] = []  # already-gifted trips

    for lucky_traveler_id in lucky_traveler_ids:
        
        # Trips not traveled by the traveler
        sql_not_tripped_trips = f"""
        select trip_package_id as trip_id
        from trip_package
        where trip_package_id not in (
            select offer_trip_package_id
            from reservation
            where Customer_id = {lucky_traveler_id}
        ) and exists (
            select destination_destination_id
            from trip_package_has_destination
            where trip_package_trip_package_id = trip_package_id
        )
        """
        cursor.execute(sql_not_tripped_trips)
        # Keeping a list of the trip_ids, so as to not mess with dicts
        not_tripped_trips_ids: list[int] = [
            not_tripped_trip_dict['trip_id'] 
            for not_tripped_trip_dict in cursor.fetchall()
        ]

        # Choosing a not-already-gifted trip 
        while(True):
            gift_trip_id: int = random.choice(not_tripped_trips_ids)
            if gift_trip_id not in gift_trip_ids: 
                gift_trip_ids.append(gift_trip_id)
                break
        

        sql_traveler_reservations = f"""
        select count(*) as num_of_rsvs
        from reservation
        where Customer_id = {lucky_traveler_id}
        """
        cursor.execute(sql_traveler_reservations)
        num_of_rsvs: int = cursor.fetchone()['num_of_rsvs']


        sql_cost_per_person = f"""
        select cost_per_person
        from trip_package
        where trip_package_id = {gift_trip_id}
        """
        cursor.execute(sql_cost_per_person)
        cost_per_person: float = cursor.fetchone()['cost_per_person']
        cost_per_person = 0.75*cost_per_person if num_of_rsvs > 1 else cost_per_person


        sql_max_offer_id = f"""
        select max(offer_id) as max_id
        from offer
        """
        cursor.execute(sql_max_offer_id)
        offer_id: int = 1 + cursor.fetchone()['max_id']


        from datetime import date, timedelta
        offer_start = date.today()
        offer_end = offer_start + timedelta(days=30)

        
        category = 'group-discount' if num_of_rsvs > 1 else 'full-price'


        sql_insert_offer = f"""
        insert into offer
        values (
            {offer_id},
            '{offer_start}',
            '{offer_end}',
            {cost_per_person},
            'Happy traveler tour',
            {gift_trip_id},
            '{category}'
        )
        """
        try:
            cursor.execute(sql_insert_offer)
            db.commit()
        except Exception as e:
            print(e)
            db.rollback()


        sql_traveler_data = f"""
        select name, surname, gender
        from traveler where traveler_id = {lucky_traveler_id}
        """
        cursor.execute(sql_traveler_data)
        traveler = cursor.fetchone()


        sql_dests = f"""
        select name
        from destination, trip_package_has_destination
        where 
            destination_id = destination_destination_id and
            trip_package_trip_package_id = {gift_trip_id}
        """
        cursor.execute(sql_dests)
        dests: str = ', '.join([dest_dict['name'] for dest_dict in cursor.fetchall()])


        result_messages.append((f"""
        Congratulations {'Mr' if traveler['gender'] == 'male' else 'Ms'} {traveler['name']} {traveler['surname']}!
        Pack your bags and get ready to enjoy the Happy traveler tour! At ART TOUR travel we
        acknowledge you as a valued customer and we 've selected the most incredible
        tailor-made travel package for you. We offer you the chance to travel to destination
        {dests} at the incredible price of {cost_per_person}. Our offer ends on {offer_end}. Use code
        OFFER{offer_id} to book your trip. Enjoy these holidays that you deserve so much!
        """,))

    return [("Lucky Winners",)] + result_messages

