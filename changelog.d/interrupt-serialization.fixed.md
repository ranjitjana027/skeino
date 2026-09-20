Interrupts now reach clients as data instead of as text. LangGraph's `Interrupt`
is a slotted dataclass, which the outbound serializer could not introspect, so
it fell back to `str()` and streamed the Python repr — an approval UI reading
`interrupt.value` found a string. Dataclasses are now serialized from their
declared fields, in both the streaming and state-snapshot serializers.
