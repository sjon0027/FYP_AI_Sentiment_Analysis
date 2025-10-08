<template>
  <div class="sectionpage text-center">
    <p class="sectionheading">Dashboard</p>
  </div>
  <ScoresChart />
  <LikertChart />
  <SpiderChart />
  <ColourPalette />
</template>
<script setup>
import ColourPalette from '@/components/ColourPalette.vue'
import ScoresChart from '@/components/ScoresChart.vue'
import LikertChart from '@/components/LikertChart.vue'
import SpiderChart from '@/components/SpiderChart.vue'

//todo on certain button click make change to use different spec
</script>
