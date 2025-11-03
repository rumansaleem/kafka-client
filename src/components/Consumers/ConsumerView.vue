<script setup lang="ts">
import { Ref, ref, watch, watchEffect } from "vue";
import { FetchOffset, GroupOffset, JsonMessageEnvelope, MessageEnvelope, consumeTopicBetweenOffsets, stopConsumer } from "@/lib/kafka";
import { cn, getLang, jsonText } from "@/lib/utils";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";
import { Label } from "../ui/label";
import { type DateValue, toCalendarDateTime, parseTime, now, DateFormatter, getLocalTimeZone } from "@internationalized/date";
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { CalendarIcon } from "lucide-vue-next";
import { Calendar } from "../ui/calendar";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";

const props = defineProps<{
  topic: string;
}>();

const messages = ref<JsonMessageEnvelope[]>([]);

const modalMessage = ref<JsonMessageEnvelope | undefined>();
const isStartConsumerModalOpen = ref(false);
const isOpen = ref(false);
const openModal = (message: JsonMessageEnvelope) => {
  modalMessage.value = message;
  isOpen.value = true;
}
const cleanUpModal = () => {modalMessage.value = undefined;}

const {toast} = useToast();

const isConsuming = ref(false);
const consumerId = ref<string>();
const consumerSubscriptionCleanUp = ref<() => void>();
const stop = async () => {
  if (consumerId.value) {
    await stopConsumer(consumerId.value);
    consumerId.value = undefined;
    consumerSubscriptionCleanUp.value && consumerSubscriptionCleanUp.value();
    isConsuming.value = false;
  }
}

const df = new DateFormatter(getLang(), {
  dateStyle: "long",
  timeStyle: "medium",
});
const tf = new DateFormatter(getLang(), {
  timeStyle: "medium",
  hour12: false
})
const offsetType = ref<FetchOffset["type"]>("End");
const offsetTimestamp = ref<DateValue>(now(getLocalTimeZone()));

function fetchMessage() {
  if (isConsuming.value) {
    return;
  }

  isConsuming.value = true;
  consumerId.value = undefined;
  const start: FetchOffset = offsetType.value === 'Timestamp'
    ? {type: offsetType.value, content: offsetTimestamp.value.toDate(getLocalTimeZone()).getTime()*1000 }
    : {type: offsetType.value};

  consumeTopicBetweenOffsets(props.topic, start, {type: "End"})
    .then(async ([event_channel, offsets]) => {
      console.log("start from", {offsets});
      consumerId.value = event_channel;
    })
    .catch((err) => {
      toast({title: "Error", description: ""+err, variant:"destructive"});
      isConsuming.value = false;
    });
}

const updateTime = (val: string) => {
  const time = parseTime(val);
  const dt = toCalendarDateTime(offsetTimestamp.value as DateValue, time);
  console.log("updateTimeValue: ", dt);
  // offsetTimestamp.value.set({hour: time.hour});
  // offsetTimestamp.value.set({minute: time.minute});
  // offsetTimestamp.value.set({second: time.second});
  // offsetTimestamp.value.set({millisecond: time.millisecond});
}

watchEffect(async () => {
  if (consumerId.value) { // subscribe on obtaining the consumerId
    messages.value = [];
    const unlisten = await getCurrentWebviewWindow().listen<MessageEnvelope|null>(consumerId.value, (evt) => {
      console.log("Listened", {evt});
      if (!evt.payload) { // Tombstone Payload
        unlisten();
        isConsuming.value = false;
        return;
      }
      messages.value.push({...evt.payload, payloadJson: jsonText(evt.payload.payload)});
    });
    // console.log("Subscribed at", new Date());
    consumerSubscriptionCleanUp.value = () => {
      unlisten();
      isConsuming.value = false;
    }
    return consumerSubscriptionCleanUp.value;
  }
});

watch(() => props.topic, (newTopic, oldTopic) => {
  if (newTopic) {
    // topic changed, cleanup.
    messages.value = [];
  }

  if (oldTopic!=newTopic) {
    (isConsuming.value || consumerId.value !=null) && stop();
  }
});
</script>

<template>
  <header class="px-4 py-2 bg-background sticky top-0 flex items-center justify-between z-10">
    <div>
      <h2 class="text-xl font-bold mb-2 flex items-center space-x-2">
        <span v-text="topic"></span>
      </h2>
    </div>
    <Dialog v-model:open="isStartConsumerModalOpen">
      <DialogTrigger as-child>
        <Button :disabled="isConsuming">Consume</Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Start New Consumer</DialogTitle>
          <DialogDescription
            >Choose confiugration to start consuming from the topic `{{ props.topic }}`
          </DialogDescription>
        </DialogHeader>
        <form id="consumer-creation-form" @submit.prevent="() => fetchMessage()">
          <div class="grid gap-2">
            <Label for="group_id">
              Offset
            </Label>
            <div class="flex space-x-2">
              <Select v-model:model-value="offsetType">
                <SelectTrigger class="w-[180px]">
                  <SelectValue placeholder="Choose Offset Type" />
                </SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value="End">
                      Latest
                    </SelectItem>
                    <SelectItem value="Beginning">
                      Earliest
                    </SelectItem>
                    <SelectItem value="Timestamp">
                      By Timestamp
                    </SelectItem>
                  </SelectGroup>
                </SelectContent>
              </Select>
              <Popover v-if="offsetType === 'Timestamp'">
                <PopoverTrigger as-child>
                  <Button
                    variant="outline"
                    :class="
                      cn(
                        'w-[280px] justify-start text-left font-normal',
                        !offsetTimestamp && 'text-muted-foreground'
                      )
                    "
                  >
                    <CalendarIcon class="mr-2 h-4 w-4" />
                    {{
                      offsetTimestamp
                        ? df.format(offsetTimestamp.toDate(getLocalTimeZone()))
                        : "Pick a date"
                    }}
                  </Button>
                </PopoverTrigger>
                <PopoverContent class="w-auto p-0">
                  <Calendar v-model="offsetTimestamp" initial-focus />
                  <Input
                    type="time"
                    :value="tf.format(offsetTimestamp.toDate(getLocalTimeZone()))"
                    @update="updateTime"
                  />
                </PopoverContent>
              </Popover>
            </div>
          </div>
        </form>
        <DialogFooter>
          <DialogClose>Cancel</DialogClose>
          <Button form="consumer-creation-form" type="submit">Start</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
    <Button v-if="isConsuming" @click="stop">
      Stop
    </Button>
  </header>

  <main class="p-4">
    <ul class="space-y-4">
      <li
        v-for="currentMessage in messages"
        :key="currentMessage.payload"
        class="bg-neutral-100 shadow rounded-md p-2"
      >
        <ul
          v-if="Object.keys(currentMessage.headers).length > 0"
          class="space-x-2 flex items-center flex-wrap mb-2"
        >
          <li v-for="key in Object.keys(currentMessage.headers)">
            <span v-text="key" class="px-2 py-1 text-xs bg-neutral-300 rounded-full"></span>
          </li>
        </ul>
        <pre
          @click="() => openModal(currentMessage)"
          class="truncate mb-2"
        ><code v-text="currentMessage.payload"></code></pre>
        <footer
          class="-mx-2 -mb-2 px-2 py-1 border-t border-neutral-300 text-xs flex justify-between"
        >
          <div class="space-x-3 flex items-baseline">
            <p>Key: "{{ currentMessage.key }}"</p>
            <p>Partition: {{ currentMessage.partition }}</p>
            <p>Offset: {{ currentMessage.offset }}</p>
          </div>
          <div class="space-x-2 flex items-baseline">
            <p>{{ new Date(currentMessage.timestamp).toLocaleDateString() }}</p>
            <p>{{ new Date(currentMessage.timestamp).toLocaleTimeString() }}</p>
          </div>
        </footer>
      </li>
    </ul>
    <Dialog
      v-model:open="isOpen"
      @update:open="
        value => {
          if (!value) {
            cleanUpModal();
          }
        }
      "
    >
      <DialogContent>
        <DialogHeader>
          <DialogTitle>
            Message
          </DialogTitle>
          <DialogDescription
            >Received on {{ modalMessage?.partition }}@{{ modalMessage?.offset }}</DialogDescription
          >
        </DialogHeader>
        <div class="mt-2 p-3 rounded-lg bg-neutral-100 w-full overflow-auto">
          <pre
            class="text-sm text-gray-500"
          ><code v-text="JSON.stringify(modalMessage?.payloadJson, null, 2)"></code></pre>
        </div>
      </DialogContent>
    </Dialog>
  </main>
</template>
