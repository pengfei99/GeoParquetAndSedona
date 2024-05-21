import json

# Sample JSON response
response = {
    "code": "Ok",
    "routes": [
        {
            "geometry": "g`~hHc_bM^eDiRk\\zVinBrIqSdBy_AcIge@vHi\\bAyc@o_@}_B_W_i@{Ik`@tWua@dIaS`U{rAnBemAs@}oAbEan@lGeK`RgGfh@p@nUfFbi@b_@dY~Iz_@fBxj@oDrl@ee@vNgSbN__@lQk{@lLsRpL_I",
            "legs": [
                {
                    "steps": [],
                    "summary": "",
                    "weight": 1115.1,
                    "duration": 1115.1,
                    "distance": 18677.9
                }
            ],
            "weight_name": "routability",
            "weight": 1115.1,
            "duration": 1115.1,
            "distance": 18677.9
        }
    ],
    "waypoints": [
        {
            "hint": "zrIqgdCyKoEIAAAARwAAANsAAAAAAAAAD1xjQMnb60GQULZCAAAAAAgAAABHAAAA2wAAAAAAAABtIgAAEDwjAMfs6AIvPCMAYO3oAgkAHwbkNR0k",
            "distance": 17.166158355,
            "name": "",
            "location": [2.309136, 48.819399]
        },
        {
            "hint": "V5UEgP___38OAAAAMQAAAJQBAACcAgAAzx6vQSYxWULcY0pElQ6ZRA4AAAAxAAAAlAEAAJwCAABtIgAAja8lAD8B6ALapSUAKP_nAhIAHwrkNR0k",
            "distance": 191.955244684,
            "name": "",
            "location": [2.469773, 48.759103]
        }
    ]
}

# Extract the first route's distance and duration
first_route = response['routes'][0]

distance = first_route['distance']
duration = first_route['duration']

print(f"Distance: {distance} meters")
print(f"Duration: {duration} seconds")