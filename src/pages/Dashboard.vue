<template>
  <div class="sectionpager px-8 py-6">
    <!-- Section Heading -->
    <p class="sectionheading text-center">Dashboard</p>

    <div class="mb-10">
      <p class="text-lg font-semibold mb-3 text-charcoal">
        General Views on Artificial Intelligence
      </p>
      <div class="bg-white border-l-4 border-lightaqua p-4 rounded-md shadow-sm">
        <p class="font-semibold text-midblue mb-2">Related Survey Questions:</p>
        <ul class="list-disc pl-6 space-y-1 text-charcoal">
          <li>
            Q1: Overall, how do you feel about the use of AI technologies in Australian law
            enforcement and national security?
          </li>
          <li>
            Q2: How familiar are you with how AI is currently used by Australian law enforcement?
          </li>
          <li>
            Q3: How much do you trust Australian law enforcement agencies to use AI technologies
            responsibly?
          </li>
          <li>
            Q4: In your own words, how do you feel about the use of AI by law enforcement (police,
            security) in Australia?
          </li>
        </ul>
      </div>
    </div>

    <!-- Charts Row -->
    <div class="grid grid-cols-1 md:grid-cols-3 gap-8 mb-10 justify-items-center">
      <!-- Chart 1 -->
      <div class="text-center w-80% p-2">
        <p class="font-semibold mb-2">Q1: Overall Sentiment</p>
        <div ref="question1"></div>
      </div>

      <!-- Chart 2 -->
      <div class="text-center w-80% p-2">
        <p class="font-semibold mb-2">Q2: Familiarity with AI Use</p>
        <div ref="question2"></div>
      </div>

      <!-- Chart 3 -->
      <div class="text-center w-80% p-2">
        <p class="font-semibold mb-2">Q3: Trust in AI Use</p>
        <div ref="question3"></div>
      </div>
    </div>

    <!-- Other Row -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-8 mb-10 justify-items-center">
      <!-- Chart 1 -->
      <div class="text-center w-80% p-2">
        <p class="font-semibold mb-2">Correlation Between Q1 and Q3</p>
        <div ref="scatterplot"></div>
      </div>

      <!-- Chart 2 -->
      <div class="text-center w-80% p-2">
        <p class="font-semibold mb-2">Comparison Across Questions</p>
        <div ref="comparisonchart"></div>
      </div>

      <!-- Chart 3 -->
    </div>

    <!-- Scatter Plot -->
    <!-- <div class="">
      <p class="font-semibold mb-2">Correlation Between Q1 and Q3</p>
      <div ref="scatterplot"></div>
    </div> -->

    <!-- Comparison Plot -->
    <!-- <div class="">
      <p class="font-semibold mb-2">Comparison Across Groups</p>
      <div ref="comparisonchart"></div>
    </div> -->

    <!-- Next Section -->
    <div class="text-center">
      <p class="font-semibold text-lg mb-2">Next Section</p>
      <div ref="chart"></div>
    </div>
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
