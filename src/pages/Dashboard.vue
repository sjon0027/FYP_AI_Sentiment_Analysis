<template>
  <div class="sectionpager px-8 py-6">
    <!-- Section Heading -->
    <p class="sectionheading text-center">Dashboard</p>
    <GeneralSection></GeneralSection>
    <LikertSection></LikertSection>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import embed from 'vega-embed'
import GeneralSection from '@/components/GeneralSection.vue'
import LikertSection from '@/components/LikertSection.vue'
</script>
