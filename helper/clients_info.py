# Clients Info

lead_clients = [ 'vastu', ] 
                

fb_coaching_client = ["vastu"]


fb_clients = fb_coaching_client # + fb_ecom_client

fb_attribution_windows = {}

for client in fb_clients:
    fb_attribution_windows[client] = []


fb_clients_level = {}
for client in fb_clients:
    fb_clients_level[client] = 'campaign'


# GA Clients

ga_clients = {'vastu': '7531936474'}

ga_client_level_campaign = []