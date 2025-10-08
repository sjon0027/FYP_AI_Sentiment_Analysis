<template>
  <div class="sectionpager">
    <p class="sectionheading text-center">Dashboard</p>

    <p>General Views on Artificial Intelligence</p>
    <p>Related Questions in Survey:</p>
    <p>
      Q1: Overall, how do you feel about the use of AI technologies in Australian law enforcement
      and national security?
    </p>
    <p>Q2: How familiar are you with how AI is currently used by Australian law enforcement?</p>
    <p>
      Q3: How much do you trust Australian law enforcement agencies to use AI technologies
      responsibly?
    </p>
    <p>
      Q4: In your own words how do you feel about the use of AI by law enforcement (police,
      security) in Australia?
    </p>
    <!-- Plots showing histogram of results to q1-3 -->
    <p>Q1</p>
    <div ref="question1"></div>
    <p>Q2</p>
    <div ref="question2"></div>
    <p>Q3</p>
    <div ref="question3"></div>
    <!-- Scatter plot comparing q1/q3 -->
    <div ref="scatterplot"></div>
    <!-- Comparison Plots -->
    <div ref="comparisonchart"></div>

    <P>Next Section</P>
    <div ref="chart"></div>
    <!-- To be completed later - perhaps maybe have each section in a tab at the top of the page however i already have a site header that has this placed as dashboard and i'd like it to remain that way -->
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import embed from 'vega-embed'
import q1Histogram from '../assets/schema_histogram_1.json'
import q2Histogram from '../assets/schema_histogram_2.json'
import q3Histogram from '../assets/schema_histogram_3.json'

import topTermsQ from '../assets/schema_top_terms_questions.json'
import topTerms from '../assets/schema_top_terms.json'
import bubble from '../assets/schema_bubble.json'
import stacked from '../assets/schema_stacked.json'
import scatter from '../assets/schema_scatter.json'
import comparison from '../assets/schema_comparison.json'

const chart = ref(null)
const scatterplot = ref(null)
const comparisonchart = ref(null)
const question1 = ref(null)
const question2 = ref(null)
const question3 = ref(null)

onMounted(() => {
  const spec = stacked
  const spec2 = scatter
  const spec3 = comparison
  const q1 = q1Histogram
  const q2 = q2Histogram
  const q3 = q3Histogram

  embed(chart.value, spec, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })
  embed(scatterplot.value, spec2, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })
  embed(comparisonchart.value, spec3, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })

  embed(question1.value, q1, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })
  embed(question2.value, q2, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })
  embed(question3.value, q3, {
    actions: { export: true, source: true, compiled: true, editor: true },
  })
})
</script>
