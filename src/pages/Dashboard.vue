<template>
  <div class="sectionpage text-center">
    <p class="sectionheading">Dashboard</p>
    <div ref="chart"></div>
    <div ref="chart2"></div>
    <div ref="chart3"></div>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import embed from 'vega-embed'
import topTermsQ from '../assets/schema_top_terms_questions.json'
import topTerms from '../assets/schema_top_terms.json'
import bubble from '../assets/schema_bubble.json'

const chart = ref(null)
const chart2 = ref(null)
const chart3 = ref(null)

onMounted(() => {
  const spec = topTermsQ
  const spec2 = topTerms
  const spec3 = bubble

  embed(chart.value, spec, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })
  embed(chart2.value, spec2, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })
  embed(chart3.value, spec3, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })
})
</script>
