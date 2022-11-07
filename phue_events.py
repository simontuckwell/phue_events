import logging
import aiohttp
import asyncio
import json
from enum import Enum, unique
from datetime import datetime
from aiohttp.client_exceptions import ClientError

log = logging.getLogger()

@unique
class HueClipVersion(Enum):
    CLIPv1 = 1
    CLIPv2 = 2


@unique
class HueEventType(Enum):
    BridgeHome = "bridge_home"
    Button = "button"
    Device = "device"
    DevicePower = "device_power"
    GeofenceClient = "geofence_client"
    GroupedLight = "grouped_light"
    Light = "light"
    LightLevel = "light_level"
    Motion = "motion"
    Scene = "scene"
    Temperature = "temperature"
    ZigbeeConnectivity = "zigbee_connectivity"

    @classmethod
    def event_types(cls):
        return [ member.value for (name, member) in cls.__members__.items() ]


@unique
class HueDevicePowerBatteryState(Enum):
    Critical = "critical"
    Low = "low"
    Normal = "normal"


@unique
class HueButtonLastEvent(Enum):
    DoubleShortRelease = "double_short_release"
    InitialPress = "initial_press"
    LongPress = "long_press"
    LongRelease = "long_release"
    Repeat = "repeat"
    ShortRelease = "short_release"


@unique
class HueZigbeeConnectivityStatus(Enum):
    Connected = "connected"
    ConnectivityIssue = "connectivity_issue"
    Disconnected = "disconnected"
    UnidirectionalIncoming = "unidirectional_incoming"


class HueEvent():
    #
    # Represents a single event received from the Hue event stream.
    #

    # Set by subclasses to reflect their association.
    _event_type = None
    
    # Choose v1 or v2 ids from the messages (v2 is the default).
    _id_version = HueClipVersion.CLIPv2
    
    @classmethod
    def _get_all_subclasses(cls):
        for subclass in cls.__subclasses__():
            yield subclass
            for subclass in subclass._get_all_subclasses():
                yield subclass

    @classmethod
    def create_class_map(cls):
        return {
            subclass._event_type: subclass \
                for subclass in HueEvent._get_all_subclasses()
            }

    def __init__(self, metadata, event_data):
        if type(self).__name__ != "HueEvent":
            log.debug("{}:{}".format(type(self).__name__, event_data))
        try:
            self._metadata = metadata
            self._event_data = event_data
            self._id_v1 = self._event_data["id_v1"] if "id_v1" in self._event_data else None
            self._id_v2 = self._event_data["id"] if "id" in self._event_data else None
            self._owner_rid = self._event_data["owner"]["rid"] if "owner" in self._event_data else None
            self._owner_rtype = self._event_data["owner"]["rtype"] if "owner" in self._event_data else None
        except Exception as e:
            log.exception("An exception occurred in {}".format(type(self).__name__), exc_info=e)

    def __dict__(self):
        return self._event_data
    
    def __repr__(self):
        return "HueEvent: {}".format(self._event_data)

    @classmethod
    def event_type(cls) -> HueEventType:
        return cls._event_type

    @classmethod
    def id_version(cls) -> HueClipVersion:
        return cls._id_version
    
    @classmethod
    def set_id_version(cls, id_version: HueClipVersion.CLIPv2):
        cls._id_version = id_version
    
    @property
    def id_v1(self) -> dict:
        if self._id_v1.count("/") == 2:
            id_v1 = self._id_v1.split("/")[-1]
        else:
            id_v1 = None
        return id_v1

    @property
    def id_v2(self) -> str:
        return self._id_v2

    @property
    def id(self) -> str:
        return self.id_v2 if self._id_version==HueClipVersion.CLIPv2 else self.id_v1

    @property
    def path(self) -> str:
        if self._id_version == HueClipVersion.CLIPv1:
            return self._id_v1
        elif self._id_version == HueClipVersion.CLIPv2:
            return "/resource/" + self._event_type.value + "/" + self._id_v2
        else:
            return None

        
class HueEventBridgeHome(HueEvent):

    _event_type = HueEventType.BridgeHome

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def children(self) -> dict:
        return self._event_data["bridge_home"]["children"]


class HueEventButton(HueEvent):

    _event_type = HueEventType.Button

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def last_event(self) -> HueButtonLastEvent:
        return HueButtonLastEvent(self._event_data["button"]["last_event"])


class HueEventDevice(HueEvent):

    _event_type = HueEventType.Device

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def identify(self):
        return self._event_data["identify"]

    @property
    def metadata(self):
        return self._event_data["metadata"]

    @property
    def product_data(self):
        return self._event_data["product_data"]

    @property
    def product_name(self):
        return self._event_data["product_name"]

    @property
    def services(self):
        return self._event_data["services"]


class HueEventDevicePower(HueEvent):

    _event_type = HueEventType.DevicePower

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def _power_state_data(self) -> int:
        return self._event_data["power_state"]
    
    @property
    def battery_level(self) -> int:
        return self._power_state_data["battery_level"]
    
    @property
    def battery_state(self) -> HueDevicePowerBatteryState:
        return HueDevicePowerBatteryState(self._power_state_data["battery_state"])


class HueEventLight(HueEvent):

    _event_type = HueEventType.Light

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def light_id(self) -> dict:
        return self.id

    @property
    def on_in_msg(self) -> bool:
        return "on" in self._event_data

    @property
    def ct_in_msg(self) -> bool:
        return "color_temperature" in self._event_data

    @property
    def color_in_msg(self) -> bool:
        return "color" in self._event_data

    @property
    def dimming_in_msg(self) -> bool:
        return "dimming" in self._event_data

    @property
    def on(self) -> bool:
        return self._event_data["on"]["on"] if self.on_in_msg else None

    @property
    def xy(self) -> dict:
        return self._event_data["color"]["xy"] if self.color_in_msg else None

    @property
    def mirek(self) -> int:
        return self._event_data["color_temperature"]["mirek"] if self.ct_in_msg else None

    @property
    def mirek_valid(self) -> bool:
        return self._event_data["color_temperature"]["mirek_valid"] if self.ct_in_msg else None

    @property
    def brightness(self) -> float:
        return float(self._event_data["dimming"]["brightness"]) if self.dimming_in_msg else None


class HueEventGroupedLight(HueEventLight):

    _event_type = HueEventType.GroupedLight

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def light_id(self):
        return None

    @property
    def group_id(self):
        return self.id


class HueEventGeofenceClient(HueEvent):

    _event_type = HueEventType.GeofenceClient

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> dict:
        return self._event_data["name"]
    

class HueEventLightLevel(HueEvent):

    _event_type = HueEventType.LightLevel

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def _light_data(self) -> dict:
        return self._event_data["light"]
    
    @property
    def light_level_valid(self) -> bool:
        return self._light_data["light_level_valid"]

    @property
    def light_level(self) -> int:
        return self._light_data["light_level"]


class HueEventMotion(HueEvent):

    _event_type = HueEventType.Motion

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def _motion_data(self) -> dict:
        return self._event_data["motion"]

    @property
    def motion_valid(self) -> bool:
        return self._motion_data["motion_valid"]

    @property
    def motion(self) -> bool:
        return self._motion_data["motion"]


class HueEventScene(HueEvent):

    _event_type = HueEventType.Scene

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def actions_in_msg(self) -> bool:
        return "actions" in self._event_data

    @property
    def auto_dynamic_in_msg(self) -> bool:
        return "auto_dynamic" in self._event_data

    @property
    def group_in_msg(self) -> bool:
        return "group" in self._event_data

    @property
    def metadata_in_msg(self) -> bool:
        return "metadata" in self._event_data

    @property
    def palette_in_msg(self) -> bool:
        return "palette" in self._event_data

    @property
    def speed_in_msg(self) -> bool:
        return "speed" in self._event_data

    @property
    def actions(self) -> dict:
        return self._event_data["actions"] if self.actions_in_msg else None

    @property
    def auto_dynamic(self) -> dict:
        return self._event_data["auto_dynamic"] if self.auto_dynamic_in_msg else None

    @property
    def group(self) -> dict:
        return self._event_data["group"] if self.group_in_msg else None

    @property
    def metadata(self) -> dict:
        return self._event_data["metadata"] if self.metadata_in_msg else None

    @property
    def palette(self) -> dict:
        return self._event_data["palette"] if self.palette_in_msg else None

    @property
    def speed(self) -> float:
        return float(self._event_data["speed"]) if self.speed_in_msg else None


class HueEventTemperature(HueEvent):

    _event_type = HueEventType.Temperature

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def _temperature_data(self) -> dict:
        return self._event_data["temperature"]
    
    @property
    def temperature(self) -> float:
        return self._temperature_data["temperature"]
    
    @property
    def temperature_valid(self) -> bool:
        return self._temperature_data["temperature_valid"]


class HueEventZigbeeConnectivity(HueEvent):

    _event_type = HueEventType.ZigbeeConnectivity

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def _zigbee_connectivity_data(self):
        return self._event_data["zigbee_connectivity"]

    @property
    def status(self):
        return HueZigbeeConnectivityStatus(self._zigbee_connectivity_data["status"])


class HueEventClassFactory():
    #
    # The factory returns an instantiated HueEvent subclass of the correct type.
    #

    # The map transforms an event type to its representative class.
    class_map = HueEvent.create_class_map()

    def __new__(cls, event_metadata, event_data) -> HueEvent:
        event_class = None
        try:
            if event_data["type"] in HueEventType.event_types():
                event_type = HueEventType(event_data["type"])
                if event_type in cls.class_map:
                    event_class = cls.class_map[event_type](event_metadata, event_data)
                else:
                    log.error(f"An unmapped event type was encountered in {event_data}")
            else:
                log.error(f"An unknown event type was encountered in {event_data}")
        except Exception as e:
            log.exception("An exception occurred in {}".format(cls.__name__), exc_info=e)
        finally:
            return event_class


class HueEventMessage():
    #
    # Represents an event message received from the Hue event stream.
    # The message data contains one or more events in a list of dictionaries.
    # The iterator returns each of the dictionaries in the list.
    #

    def __init__(self, msg_id, msg_timestamp, msg_data):
        self.msg_id = msg_id
        self.msg_timestamp = msg_timestamp
        self.metadata = {
            "msg_id": self.msg_id,
            "msg_ts": self.msg_timestamp,
        }
        self.msg_content = json.loads(msg_data)[0]
        self.data = self.msg_content["data"]
        self.iter_index = 0

    def __iter__(self):
        self.iter_index = 0
        return self

    def __next__(self):
        if self.iter_index < len(self.data):
            event_data = self.data[self.iter_index]
            self.iter_index += 1
            if event_data:
                return HueEventClassFactory(self.metadata, event_data)
            else:
                return self.__next__()
        else:
            raise StopIteration

    def __dict__(self):
        return {
            "msg_id": self.msg_id,
            "msg_ts": self.msg_timestamp,
            "msg_data": self.data,
        }
    
    def __repr__(self):
        return "HueEventMessage: Id={msg_id} Timestamp={msg_ts}, Data={msg_data}".format(
            **self.__dict__())


class BridgeEventStream():
    #
    # This class provides access to the Hue event stream.
    # https://developers.meethue.com/develop/hue-api-v2/migration-guide-to-the-new-hue-api/#Event%20Stream
    #
    # * The bridge streams messages containing one or more events.
    # * Each individual event is transformed to a HueEvent object.
    # * Callback functions can be registered for each HueEventType.
    # * The HueEvent object will be passed to the callback function.
    #

    def __init__(self, ip_address, username, id_version=HueClipVersion.CLIPv2, error_sleep_time=5):
        self.event_callbacks = {}
        self.ip_address = ip_address
        self.username = username
        self.error_sleep_time = error_sleep_time
        HueEvent.set_id_version(id_version)

    def add_event_callback(self, event_type: HueEventType, event_callback):
        try:
            if event_type not in self.event_callbacks:
                self.event_callbacks[event_type] = []
            if event_callback not in self.event_callbacks[event_type]:
                self.event_callbacks[event_type].append(event_callback)
                log.debug(f"Event type {event_type} callback handler {event_callback} added")
            else:
                log.error(f"Event type {event_type} callback handler {event_callback} already known")
        except Exception as e:
            log.exception("An exception occurred in {}".format(type(self).__name__), exc_info=e)

    def remove_event_callback(self, event_type: HueEventType, event_callback):
        try:
            if event_type in self.event_callbacks and event_callback in self.event_callbacks[event_type]:
                self.event_callbacks[event_type].remove(event_callback)
                log.debug(f"Event type {event_type} callback handler {event_callback} removed")
            else:
                log.error(f"Event type {event_type} callback handler {event_callback} unknown")
        except Exception as e:
            log.exception("An exception occurred in {}".format(type(self).__name__), exc_info=e)

    async def event_dispatcher(self, msg_id, msg_data):
        # The first part of the Philips Hue message id is a timestamp in local time.
        msg_epochtime = int(msg_id.split(":")[0])
        # This converts the epoch time into a timestamp.
        # It currently assumes the Bridge is in the same timezone.
        #tz_name = self.get_timezone()
        #tz = somehow_convert(tz_name) pytz?
        msg_timestamp = datetime.fromtimestamp(msg_epochtime).astimezone()
        message = HueEventMessage(msg_id, msg_timestamp, msg_data)
        for event in message:
            try:
                # event may be None if the data contained an unknown type.
                if event:
                    event_type = HueEventType(event.event_type())
                    if event_type in self.event_callbacks:
                        for event_callback in self.event_callbacks[event_type]:
                            await event_callback(event)
            except AttributeError as e:
                log.exception(f"Unexpected event {event} passed in {message}", exc_info=e)
            except Exception as e:
                log.exception("An exception occurred in {}".format(type(self).__name__), exc_info=e)

    async def event_listener(self):
        log.debug("Bridge event listener starting")
        msg_id = ""
        msg_data = ""
        get_params = {
            "url": f"https://{self.ip_address}/eventstream/clip/v2",
            "verify_ssl": False,
            "allow_redirects": True,
            "timeout": None,
            "headers": {
                "Hue-Application-Key": self.username,
                "Accept": "text/event-stream",
            }
        }
        try:
            async with aiohttp.ClientSession() as client:
                async with client.get(**get_params) as resp:
                    async for line in resp.content:
                        l = line.decode("utf-8").strip()
                        if l.find(": ") > 0:
                            (msg, payload) = l.split(": ")
                            if msg == "id":
                                msg_id = payload
                            elif msg == "data":
                                msg_data = payload
                                await self.event_dispatcher(msg_id, msg_data)
                                msg_id = ""
                                msg_data = ""
        except ClientError as e:
            log.error(f"Connection error: {e}")
            # Back off for a while on error.
            await asyncio.sleep(self.error_sleep_time)
        except Exception as e:
            log.exception("An exception occurred in {}".format(type(self).__name__), exc_info=e)
