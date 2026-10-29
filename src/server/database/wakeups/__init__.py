"""Wakeups (``wakeups``): the agent's own follow-ups on a thread.

``contract`` is what a notice job is and when it is owed, and the only module
here the outbox imports. Setting is ``arm``, firing ``fire``, ending
``cancel``, reading ``read``, and the notices that share one wake turn
``riders``. Nothing is re-exported, so importing the contract loads no more
than the contract.
"""
