<template>
  <div class="mb-10">
    <p class="font-semibold text-lg mb-2">Likert Scale Questions</p>
    <!-- <div class="bg-white border-l-4 border-lightaqua p-4 rounded-md shadow-sm">
      <p class="font-semibold text-midblue mb-2">Statements:</p>
      <ul class="list-disc pl-6 space-y-1 text-charcoal">
        <li>AI in law enforcement threatens personal privacy.</li>
        <li>
          AI in law enforcement reduces my trust because of how personal data is collected and used.
        </li>
        <li>
          AI in law enforcement reduces my trust because of increased monitoring (e.g., facial
          recognition, tracking).
        </li>
        <li>There is not enough accountability for how AI is used in law enforcement.</li>
        <li>
          If an AI system makes an incorrect or unfair decision, it should be easy for people to
          challenge or appeal that decision.
        </li>
        <li>AI decisions in policing should always be subject to human review.</li>
        <li>
          I find it easy to access and understand information about how AI is being used by law
          enforcement in Australia.
        </li>
        <li>
          Information about how AI is used in law enforcement should be easily accessible to the
          public.
        </li>
        <li>
          The government should involve the public in decisions about how AI is used in law
          enforcement.
        </li>
        <li>I believe AI could reduce human bias in policing compared to traditional methods.</li>

        <li>AI decision-making can reinforce bias or discrimination.</li>
        <li>AI increases the risk of misuse or abuse of power.</li>
        <li>
          I believe there are adequate legal safeguards in Australia to regulate the use of AI in
          policing and security.
        </li>
        <li>I believe AI can make law enforcement more effective in preventing crime.</li>
        <li>
          I would feel safer if AI technologies were widely used in national security in Australia.
        </li>
        <li>
          I am more concerned about the use of AI by law enforcement than its use in other areas
          (e.g., healthcare, education, business).
        </li>
      </ul>
    </div> -->
  </div>
  <div ref="chart"></div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import embed from 'vega-embed'
import topTermsQ from '../assets/schema_top_terms_questions.json'
import topTerms from '../assets/schema_top_terms.json'
import bubble from '../assets/schema_bubble.json'
import stacked from '../assets/schema_stacked.json'

const chart = ref(null)

onMounted(() => {
  const spec = stacked

  embed(chart.value, spec, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })
})
</script>
