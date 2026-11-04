"""Push notifications to the user's phones over APNs.

``config`` reads the APNs credentials, ``apns`` sends one notification, and
``notify`` is the ``push`` outbox executor that decides what a settled run
tells which device.
"""
