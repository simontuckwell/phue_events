#!/usr/bin/python3

# phue_events example
# Print a message when a group of lights is switched on.

import asyncio
import phue_events
from phue_events import BridgeEventStream, HueEventType, HueEventGroupedLight

BRIDGE_IP = "<your bridge ip>"
APP_KEY = "<your app key>"

events = BridgeEventStream(BRIDGE_IP, APP_KEY)

async def process_grouped_light_event(event: HueEventGroupedLight):
    if event.on:
        print(f"Group On event for {event.group_id}.")

async def event_task():
    events.add_event_callback(HueEventType.GroupedLight, process_grouped_light_event)
    while True:
        await events.event_listener()

async def main():
    await asyncio.create_task(self.event_task())

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
